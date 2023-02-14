-module(riak_crypto).

-export([sign_v4/7, hash_password/1, check_password/3, uuid4/0, seed/0, random_string/0, md5/1]).

-include("riak.hrl").

-define(SALT_LENGTH, 16).
-define(HASH_ITERATIONS, 4096).
-define(HASH_FUNCTION, sha256).


-type headers() :: [{string(), string()}].

%%
%% Signs request using AWS Signature Version 4.
%%
-spec sign_v4(atom(), list(), headers(), binary(), string(), string(), list()) -> headers().

sign_v4(Method, Uri, Headers0, Payload, Region, Service, QueryParams) ->
    Date = iso_8601_basic_time(),
    Config = #riak_api_config{},
    {PayloadHash, Headers1} =
        sign_v4_content_sha256_header( [{"x-amz-date", Date} | Headers0], Payload ),
    {Request, SignedHeaders} = canonical_request(Method, Uri, QueryParams, Headers1, PayloadHash),

    DateOnly = string:left(Date, 8),
    CredentialScope = [DateOnly, $/, Region, $/, Service, "/aws4_request"],
    ToSign = ["AWS4-HMAC-SHA256\n", Date, $\n, CredentialScope, $\n, hash_encode(Request)],
    %% TODO cache the signing key so we don't have to recompute for every request
    KDate = sha256_mac( "AWS4" ++ Config#riak_api_config.secret_access_key, DateOnly),
    KRegion = sha256_mac( KDate, Region),
    KService = sha256_mac( KRegion, Service),
    SigningKey = sha256_mac( KService, "aws4_request"),

    Signature = base16(sha256_mac( SigningKey, ToSign)),
    Authorization = ["AWS4-HMAC-SHA256"
     " Credential=", Config#riak_api_config.access_key_id, $/, CredentialScope, $,,
     " SignedHeaders=", SignedHeaders, $,,
     " Signature=", Signature],
    [{"Authorization", lists:flatten(Authorization)} | Headers1].

sign_v4_content_sha256_header( Headers, Payload ) ->
    case proplists:get_value( "x-amz-content-sha256", Headers ) of
        undefined ->
            PayloadHash = hash_encode(Payload),
            NewHeaders = [{"x-amz-content-sha256", PayloadHash} | Headers],
            {PayloadHash, NewHeaders};
        PayloadHash -> {PayloadHash, Headers}
    end.

iso_8601_basic_time() ->
    {{Year,Month,Day},{Hour,Min,Sec}} = calendar:now_to_universal_time(os:timestamp()),
    lists:flatten(io_lib:format(
                    "~4.10.0B~2.10.0B~2.10.0BT~2.10.0B~2.10.0B~2.10.0BZ",
                    [Year, Month, Day, Hour, Min, Sec])).

canonical_request(Method, CanonicalURI, QParams, Headers, PayloadHash) ->
    {CanonicalHeaders, SignedHeaders} = canonical_headers(Headers),
    CanonicalQueryString = canonical_query_string(QParams),
    {[string:to_upper(erlang:atom_to_list(Method)), $\n,
      CanonicalURI, $\n,
      CanonicalQueryString, $\n,
      CanonicalHeaders, $\n,
      SignedHeaders, $\n,
      PayloadHash],
     SignedHeaders}.

canonical_headers(Headers) ->
    Normalized = [{string:to_lower(Name), trimall(Value)} || {Name, Value} <- Headers],
    Sorted = lists:keysort(1, Normalized),
    Canonical = [[Name, $:, Value, $\n] || {Name, Value} <- Sorted],
    Signed = string:join([Name || {Name, _} <- Sorted], ";"),
    {Canonical, Signed}.

%% @doc calculate canonical query string out of query params and according to v4 documentation
canonical_query_string([]) -> "";
canonical_query_string(Params) ->
    Normalized = [{erlcloud_http:url_encode(Name), erlcloud_http:url_encode(erlcloud_http:value_to_string(Value))} || {Name, Value} <- Params],
    Sorted = lists:keysort(1, Normalized),
    string:join([case Value of
                     [] -> [Key, "="];
                     _ -> [Key, "=", Value]
                 end
                 || {Key, Value} <- Sorted, Value =/= none, Value =/= undefined], "&").

trimall(Value) when erlang:is_list(Value) ->
    %% TODO - remove excess internal whitespace in header values
    re:replace(Value, "(^\\s+)|(\\s+$)", "", [global]);
trimall(Value) when erlang:is_binary(Value) -> Value;
trimall(Value) -> utils:to_list(Value).

hash_encode(Data) ->
    Hash = crypto:hash(sha256, Data),
    base16(Hash).

hex(N) when N < 10 ->
    N + $0;
hex(N) when N < 16 ->
    N - 10 + $a.

base16(Data) ->
    binary:bin_to_list(<< <<(hex(N div 16)), (hex(N rem 16))>> || <<N>> <= Data >>).

sha256_mac(K, S) ->
    try
        crypto:mac(hmac, sha256, K, S)
    catch
        error:undef ->
            R0 = crypto:mac_init(hmac, sha256, K),
            R1 = crypto:mac_update(R0, S),
            crypto:mac_final(R1)
    end.


%% @doc Hash a plaintext password, returning hashed password and algorithm details
hash_password(BinaryPass) when erlang:is_binary(BinaryPass) ->
    Salt0 = try crypto:strong_rand_bytes(?SALT_LENGTH) of
	Value -> Value
    catch
	error:low_entropy ->
	    seed(),
	    crypto:strong_rand_bytes(?SALT_LENGTH)
    end,
    % Hash the original password and store as hex
    {ok, HashedPass} = pbkdf2:pbkdf2(?HASH_FUNCTION, BinaryPass, Salt0, ?HASH_ITERATIONS),
    HexPass = utils:hex(HashedPass),
    Salt1 = utils:hex(Salt0),
    {ok, HexPass, utils:to_list(Salt1)}.


%% @doc Check a plaintext password with a hashed password
check_password(BinaryPass, HashedPassword, Salt) when erlang:is_binary(BinaryPass) ->

    % Hash BinaryPassword to compare to HashedPassword
    {ok, HashedPass} = pbkdf2:pbkdf2(?HASH_FUNCTION, BinaryPass, Salt, ?HASH_ITERATIONS),

    HexPass = utils:hex(HashedPass),
    HexPass =:= HashedPassword.


seed() ->
    Args = lists:flatten(io_lib:format("-c ~p", [16])),
    erlang:open_port({spawn_executable, "/usr/bin/head"}, [
	use_stdio, in, binary, exit_status, {args, [Args, "/dev/urandom"]}]),
    receive
	{_Port, {data, Reply}} ->
	    Reply;
	{'EXIT', _Port, _} ->
	    erlang:error(badarg)
    end.

%%
%% UUID4 is 122 bits of entropy; we need 16 8-bit bytes to get that.
%% Example UUID4: f47ac10b-58cc-4372-a567-0e02b2c3d479
%% For reasons unknown, the third group of characters must start with the number 4.
%% For further unknown reasons, the fourth group of characters must start with 8, 9, a or b.
%%
-spec uuid4() -> binary().

uuid4() ->
    RandomBytes = try crypto:strong_rand_bytes(16) of
	Value -> Value
    catch
	error:low_entropy ->
	    seed(),
	    crypto:strong_rand_bytes(16)
    end,
    <<First:32, Second:16, Third:12, Fourth:2, Fifth:12, Sixth:48, _UselessPadding:6, _Rest/binary>> = RandomBytes,
    erlang:list_to_binary(io_lib:format("~8.16.0b-~4.16.0b-4~3.16.0b-~1.16.0b~3.16.0b-~12.16.0b",
	[First, Second, Third, Fourth+8, Fifth, Sixth])).

%%
%% Generates random string.
%%
-spec random_string() -> string().

random_string() ->
    Length = 20,
    AllowedChars = "0123456789abcdefghijklmnopqrstuvwxyz",
    lists:foldl(
	fun(_, Acc) ->
            try [lists:nth(rand:uniform(length(AllowedChars)), AllowedChars)] of
		Value -> Value ++ Acc
	    catch error:low_entropy ->
		riak_crypto:seed(),
                [lists:nth(rand:uniform(length(AllowedChars)), AllowedChars)] ++ Acc
	    end
	end, [], lists:seq(1, Length)).


-spec md5(iodata()) -> binary().

md5(IOData) ->
    crypto:hash(md5, IOData).
