-module(riak_crypto).

-export([sign_v4/7, check_base58/1, to_base58/1, from_base58/1]).

-include("riak.hrl").

-type headers() :: [{string(), string()}].

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
    {[string:to_upper(atom_to_list(Method)), $\n,
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
canonical_query_string([]) ->
    "";
canonical_query_string(Params) ->
    Normalized = [{erlcloud_http:url_encode(Name), erlcloud_http:url_encode(erlcloud_http:value_to_string(Value))} || {Name, Value} <- Params],
    Sorted = lists:keysort(1, Normalized),
    string:join([case Value of
                     [] -> [Key, "="];
                     _ -> [Key, "=", Value]
                 end
                 || {Key, Value} <- Sorted, Value =/= none, Value =/= undefined], "&").

trimall(Value) ->
    %% TODO - remove excess internal whitespace in header values
    re:replace(Value, "(^\\s+)|(\\s+$)", "", [global]).

hash_encode(Data) ->
    Hash = crypto:hash(sha256, Data),
    base16(Hash).

base16(Data) ->
    binary:bin_to_list(base16:encode(Data)).

sha256_mac(K, S) ->
    try
        crypto:hmac(sha256, K, S)
    catch
        error:undef ->
            R0 = crypto:hmac_init(sha256, K),
            R1 = crypto:hmac_update(R0, S),
            crypto:hmac_final(R1)
    end.

to_base58(String) when erlang:is_list(String) ->
    base58:binary_to_base58(utils:unhex(list_to_binary(String))).

from_base58(String) when erlang:is_list(String) ->
    utils:hex(base58:base58_to_binary(String)).

check_base58(String) when erlang:is_list(String) ->
    base58:check_base58(String).
