%%
%% This module contains common utilities.
%%
-module(utils).
-export([mime_type/1,
	 slugify_object_key/1, prefixed_object_key/2, alphanumeric/1, trim_spaces/1,
	 is_valid_bucket_id/2, is_valid_object_key/1, is_bucket_belongs_to_group/3,
	 is_bucket_belongs_to_tenant/2, to_integer/1, to_integer/2, to_float/1,
	 to_float/2, to_number/1, to_list/1, to_binary/1, to_atom/1, to_boolean/1,
	 is_true/1, is_false/1, even/1, has_duplicates/1, ends_with/2, starts_with/2,
	 hex/1, unhex/1, unhex_path/1, validate_utf8/2, is_valid_hex_prefix/1,
	 is_hidden_object/1, join_list_with_separator/3, check_token/1,
	 to_lower/1, timestamp/0]).

-include("riak.hrl").
-include("general.hrl").
-include("user.hrl").

mime_types() ->
    MimeTypesFile = "/etc/mime.types",
    {ok, MimeTypes} = httpd_conf:load_mime_types(MimeTypesFile),
    MimeTypes.

-spec mime_type(string()) -> string().

mime_type(FileName) when erlang:is_list(FileName) ->
    case filename:extension(FileName) of
	[] ->
	    "application/octet_stream";
	Extension ->
	    MimeTypes = mime_types(),
	    LookupKey = string:substr(Extension, 2),
	    proplists:get_value(LookupKey, MimeTypes, "application/octet_stream")
    end.

%%
%% Extracts letters and digits from binary.
%%
-spec alphanumeric(binary()|list()) -> list().

alphanumeric(String) when erlang:is_list(String) ->
    alphanumeric(erlang:list_to_binary(String));
alphanumeric(String) when erlang:is_binary(String)  ->
    re:replace(String, <<"[^a-zA-Z0-9]+">>, <<"">>, [{return, binary}, global]).

%%
%% Transliterates binary to ascii.
%%
-spec slugify_object_key(binary()) -> string().

slugify_object_key(FileName0) when erlang:is_binary(FileName0) ->
    FileName1 = filename:rootname(FileName0),
    FileName2 = slughifi:slugify(FileName1),
    %% Not all unicode characters can be transliterated
    Regex = <<"[^a-zA-Z0-9\-\._~,\\s+]+">>,
    FileName3 = re:replace(unicode:characters_to_binary(FileName2), Regex, <<"">>, [{return, binary}, global]),
    FileName4 = string:to_lower(erlang:binary_to_list(FileName3)),
    Extension0 = filename:extension(FileName0),
    Extension1 = slughifi:slugify(Extension0),
    Extension2 = re:replace(unicode:characters_to_binary(Extension1), Regex, <<"">>, [{return, binary}, global]),
    Extension3 = string:to_lower(erlang:binary_to_list(Extension2)),
    case Extension3 of
	[] -> FileName4;
	_ -> lists:concat([FileName4, Extension3])
    end.

%%
%% Removes spaces at the end and on the beginning of binary string
%%
-spec trim_spaces(binary()) -> binary().

trim_spaces(Bin0) ->
    re:replace(Bin0, <<"^\\s+|\\s+$">>, <<"">>, [{return, binary}, global]).

%%
%% Returns 'prefix/object_key'
%% or, if prefix is empty just 'object_key'
%%
-spec prefixed_object_key(string()|binary()|undefined, string()|binary()) -> string().

prefixed_object_key(undefined, ObjectKey) -> ObjectKey;
prefixed_object_key([], ObjectKey) -> ObjectKey;
prefixed_object_key(".", ObjectKey) -> ObjectKey;
prefixed_object_key(<<>>, ObjectKey) -> ObjectKey;
prefixed_object_key(Prefix, ObjectKey0) when erlang:is_binary(Prefix), erlang:is_binary(ObjectKey0) ->
    ObjectKey1 =
	case starts_with(ObjectKey0, <<"/">>) of
	    true ->
		<< _:1/binary, N0/binary >> = ObjectKey0,
		N0;
	    false -> ObjectKey0
	end,
    case ends_with(Prefix, <<"/">>) of
	true -> << Prefix/binary, ObjectKey1/binary >>;
	false -> << Prefix/binary, <<"/">>/binary, ObjectKey1/binary >>
    end;
prefixed_object_key(Prefix, ObjectKey0) when erlang:is_list(Prefix), erlang:is_list(ObjectKey0) ->
    ObjectKey1 =
	case string:sub_string(ObjectKey0, 1, 1) =:= "/" of
	    true -> string:sub_string(ObjectKey0, 2, length(ObjectKey0));
	    false -> ObjectKey0
	end,
    case string:sub_string(Prefix, length(Prefix), length(Prefix)) =:= "/" of
	%% strip '/' at the end, return 'prefix/object_key'
	true -> string:concat(Prefix, ObjectKey1);
	_ -> string:concat(Prefix ++ "/", ObjectKey1)
    end.

-spec to_integer(string() | binary() | integer() | float() | undefined) -> integer().

to_integer(X) ->
    to_integer(X, nonstrict).

-spec to_integer(string() | binary() | integer() | float(),
		 strict | nonstrict | undefined) -> integer().
to_integer(X, strict)
  when erlang:is_float(X) ->
    erlang:error(badarg);
to_integer(X, nonstrict)
  when erlang:is_float(X) ->
    erlang:round(X);
to_integer(X, S)
  when erlang:is_binary(X) ->
    to_integer(erlang:binary_to_list(X), S);
to_integer(X, S)
  when erlang:is_list(X) ->
    try erlang:list_to_integer(X) of
        Result ->
            Result
    catch
        error:badarg when S =:= nonstrict ->
            erlang:round(erlang:list_to_float(X))
    end;
to_integer(X, _)
  when erlang:is_integer(X) ->
    X.

%% @doc
%% Automatic conversion of a term into float type. badarg if strict
%% is defined and an integer value is passed.
-spec to_float(string() | binary() | integer() | float()) ->
                      float().
to_float(X) ->
    to_float(X, nonstrict).

-spec to_float(string() | binary() | integer() | float(), strict | nonstrict) -> float().

to_float(X, S) when erlang:is_binary(X) ->
    to_float(erlang:binary_to_list(X), S);
to_float(X, S)
  when erlang:is_list(X) ->
    try erlang:list_to_float(X) of
        Result ->
            Result
    catch
        error:badarg when S =:= nonstrict ->
            erlang:list_to_integer(X) * 1.0
    end;
to_float(X, strict) when
      erlang:is_integer(X) ->
    erlang:error(badarg);
to_float(X, nonstrict)
  when erlang:is_integer(X) ->
    X * 1.0;
to_float(X, _) when erlang:is_float(X) ->
    X.

%% @doc
%% Automatic conversion of a term into number type.
-spec to_number(binary() | string() | number()) ->
                       number().
to_number(X)
  when erlang:is_number(X) ->
    X;
to_number(X)
  when erlang:is_binary(X) ->
    to_number(to_list(X));
to_number(X)
  when erlang:is_list(X) ->
    try list_to_integer(X) of
        Int -> Int
    catch
        error:badarg ->
            list_to_float(X)
    end.

%% @doc
%% Automatic conversion of a term into Erlang list
-spec to_list(atom() | list() | binary() | integer() | float()) -> list().
to_list(X) when erlang:is_float(X) ->
    erlang:float_to_list(X);
to_list(X) when erlang:is_integer(X) ->
    erlang:integer_to_list(X);
to_list(X) when erlang:is_binary(X) ->
    erlang:binary_to_list(X);
to_list(X) when erlang:is_atom(X) ->
    erlang:atom_to_list(X);
to_list(X) when erlang:is_list(X) ->
    X.

%% @doc
%% Known limitations:
%%   Converting [256 | _], lists with integers > 255
-spec to_binary(atom() | string() | binary() | integer() | float()) -> binary().
to_binary(X) when erlang:is_float(X) ->
    to_binary(to_list(X));
to_binary(X) when erlang:is_integer(X) ->
    erlang:iolist_to_binary(integer_to_list(X));
to_binary(X) when erlang:is_atom(X) ->
    erlang:list_to_binary(erlang:atom_to_list(X));
to_binary(X) when erlang:is_list(X) ->
    erlang:iolist_to_binary(X);
to_binary(X) when erlang:is_binary(X) ->
    X.

-spec to_boolean(binary() | string() | atom()) ->
                        boolean().
to_boolean(<<"true">>) -> true;
to_boolean("true") -> true;
to_boolean(true) -> true;
to_boolean(<<"false">>) -> false;
to_boolean("false") -> false;
to_boolean(false) -> false.

-spec is_true(binary() | string() | atom()) -> boolean().

is_true(<<"true">>) -> true;
is_true("true") -> true;
is_true(true) -> true;
is_true(_) -> false.

-spec is_false(binary() | string() | atom()) -> boolean().

is_false(<<"false">>) -> true;
is_false("false") -> true;
is_false(false) -> true;
is_false(_) -> false.

%% @doc
%% Automation conversion a term to an existing atom. badarg is
%% returned if the atom doesn't exist.  the safer version, won't let
%% you leak atoms
-spec to_atom(atom() | list() | binary() | integer() | float()) ->
                     atom().
to_atom(X) when erlang:is_atom(X) ->
    X;
to_atom(X) when erlang:is_list(X) ->
    erlang:list_to_atom(X);
to_atom(X) ->
    to_atom(to_list(X)).

%% This function returns 0 on success, 1 on error, and 2..8 on incomplete data.
validate_utf8(<<>>, State) -> State;
validate_utf8(<< C, Rest/bits >>, 0) when C < 128 -> validate_utf8(Rest, 0);
validate_utf8(<< C, Rest/bits >>, 2) when C >= 128, C < 144 -> validate_utf8(Rest, 0);
validate_utf8(<< C, Rest/bits >>, 3) when C >= 128, C < 144 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 5) when C >= 128, C < 144 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 7) when C >= 128, C < 144 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 8) when C >= 128, C < 144 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 2) when C >= 144, C < 160 -> validate_utf8(Rest, 0);
validate_utf8(<< C, Rest/bits >>, 3) when C >= 144, C < 160 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 5) when C >= 144, C < 160 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 6) when C >= 144, C < 160 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 7) when C >= 144, C < 160 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 2) when C >= 160, C < 192 -> validate_utf8(Rest, 0);
validate_utf8(<< C, Rest/bits >>, 3) when C >= 160, C < 192 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 4) when C >= 160, C < 192 -> validate_utf8(Rest, 2);
validate_utf8(<< C, Rest/bits >>, 6) when C >= 160, C < 192 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 7) when C >= 160, C < 192 -> validate_utf8(Rest, 3);
validate_utf8(<< C, Rest/bits >>, 0) when C >= 194, C < 224 -> validate_utf8(Rest, 2);
validate_utf8(<< 224, Rest/bits >>, 0) -> validate_utf8(Rest, 4);
validate_utf8(<< C, Rest/bits >>, 0) when C >= 225, C < 237 -> validate_utf8(Rest, 3);
validate_utf8(<< 237, Rest/bits >>, 0) -> validate_utf8(Rest, 5);
validate_utf8(<< C, Rest/bits >>, 0) when C =:= 238; C =:= 239 -> validate_utf8(Rest, 3);
validate_utf8(<< 240, Rest/bits >>, 0) -> validate_utf8(Rest, 6);
validate_utf8(<< C, Rest/bits >>, 0) when C =:= 241; C =:= 242; C =:= 243 -> validate_utf8(Rest, 7);
validate_utf8(<< 244, Rest/bits >>, 0) -> validate_utf8(Rest, 8);
validate_utf8(_, _) -> 1.

%% This function returns 0 on success, 1 on error.
%% Usage: case even(byte_size(Str)) of true -> validate_hex();..
validate_hex(<<>>, State) -> State;
validate_hex(<< C, Rest/bits >>, 0) when C >= $0 andalso C =< $9 -> validate_hex(Rest, 0);
validate_hex(<< C, Rest/bits >>, 0) when C >= $a andalso C =< $f -> validate_hex(Rest, 0);
validate_hex(<< C, Rest/bits >>, 0) when C >= $A andalso C =< $F -> validate_hex(Rest, 0);
validate_hex(_, _) -> 1.

%%
%% Checks the following
%% - Bucket has specified tenant ID
%%     as system do not allow to read contents of other tenants
%% - Length of bucket is less than 63
%%     as this is limit of Riak CS
%% - Suffix is either private, public or restricted
%%
-spec is_valid_bucket_id(string(), string()) -> boolean().

is_valid_bucket_id(BucketId, TenantName)
	when erlang:is_list(BucketId), erlang:is_list(TenantName) ->
    Bits = string:tokens(BucketId, "-"),
    case length(Bits) =:= 4 andalso length(BucketId) =< 63 of
	true ->
	    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
	    BucketSuffix = lists:last(Bits),
	    BucketSuffix = lists:last(Bits),
	    (BucketSuffix =:= ?PRIVATE_BUCKET_SUFFIX
	     orelse BucketSuffix =:= ?PUBLIC_BUCKET_SUFFIX
	     orelse BucketSuffix =:= ?RESTRICTED_BUCKET_SUFFIX
	    ) andalso BucketTenantName =:= TenantName
	      andalso lists:prefix([?RIAK_BACKEND_PREFIX], Bits) =:= true;
	false ->
	    false
    end;
is_valid_bucket_id(_, _) -> false.

even(X) when X >= 0 -> (X band 1) == 0.

%%
%% Checks if provided prefix consists of allowed characters
%%
is_valid_hex_prefix(HexPrefix) when erlang:is_binary(HexPrefix) ->
    F0 = fun(T) ->
	    case even(erlang:byte_size(T)) of
		true ->
		    case validate_hex(T, 0) of
			0 -> is_valid_object_key(unhex(T));
			1 -> false
		    end;
	    _ ->
		false
	    end
	end,
    lists:all(
	F0, [T || T <- binary:split(HexPrefix, <<"/">>, [global]), erlang:byte_size(T) > 0]
    );
is_valid_hex_prefix(undefined) ->
    true.

%%
%% - Converts binary to string.
%% - Appends "/" at the end if absent.
%% - Make lowercase.
%%
-spec to_lower(binary()) -> list()|undefined.

to_lower(Data0) when erlang:is_binary(Data0) ->
    case Data0 of
	<<>> -> undefined;
	Data1 ->
	    Data2 = string:to_lower(lists:flatten(erlang:binary_to_list(Data1))),
	    case ends_with(Data1, <<"/">>) of
		true -> Data2;
		false -> Data2++"/"
	    end
    end;
to_lower(undefined) -> undefined.

%%
%% Returns true, if "tenant id" and "group name", that are encoded
%% in BucketId equal to provided TenantName and GroupName.
%% the-projectname-groupname-public
%% ^^^ ^^^^^^^^^^^ ^^^^^^^^
%% prefix  bucket  group
%%
-spec is_bucket_belongs_to_group(string(), string(), string()) -> boolean().

is_bucket_belongs_to_group(BucketId, TenantName, GroupName)
    when erlang:is_list(BucketId), erlang:is_list(TenantName),
	 erlang:is_list(GroupName) ->
    Bits = string:tokens(BucketId, "-"),
    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
    BucketGroupName = string:to_lower(lists:nth(3, Bits)),
    BucketGroupName =:= GroupName andalso BucketTenantName =:= TenantName;
is_bucket_belongs_to_group(_,_,_) -> false.

is_bucket_belongs_to_tenant(BucketId, TenantName)
	when erlang:is_list(BucketId), erlang:is_list(TenantName) ->
    Bits = string:tokens(BucketId, "-"),
    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
    BucketTenantName =:= TenantName;
is_bucket_belongs_to_tenant(_,_) -> false.

%%
%% This function returns 0 when binary string do not contain forbidden
%% characters. Returns 1 otherwise.
%%
%% Forbidden characters are " < > \ | / : * ?
%%
is_valid_object_key(<<>>, State) -> State;
is_valid_object_key(<< $", _/bits >>, 0) -> 1;
is_valid_object_key(<< $<, _/bits >>, 0) -> 1;
is_valid_object_key(<< $>, _/bits >>, 0) -> 1;
is_valid_object_key(<< "\\", _/bits >>, 0) -> 1;
is_valid_object_key(<< $|, _/bits >>, 0) -> 1;
is_valid_object_key(<< $/, _/bits >>, 0) -> 1;
is_valid_object_key(<< $:, _/bits >>, 0) -> 1;
is_valid_object_key(<< $*, _/bits >>, 0) -> 1;
is_valid_object_key(<< $?, _/bits >>, 0) -> 1;
is_valid_object_key(<< _, Rest/bits >>, 0) -> is_valid_object_key(Rest, 0).

is_valid_object_key(Prefix) when erlang:is_binary(Prefix) ->
    (size(Prefix) =< 254) andalso (validate_utf8(Prefix, 0) =:= 0) andalso (is_valid_object_key(Prefix, 0) =:= 0);
is_valid_object_key(_) ->
    false.

digit(0) -> $0;
digit(1) -> $1;
digit(2) -> $2;
digit(3) -> $3;
digit(4) -> $4;
digit(5) -> $5;
digit(6) -> $6;
digit(7) -> $7;
digit(8) -> $8;
digit(9) -> $9;
digit(10) -> $a;
digit(11) -> $b;
digit(12) -> $c;
digit(13) -> $d;
digit(14) -> $e;
digit(15) -> $f.

hex(undefined) -> [];
hex(Bin) -> erlang:binary_to_list(<< << (digit(A1)),(digit(A2)) >> || <<A1:4,A2:4>> <= Bin >>).
unhex(Hex) -> << << (erlang:list_to_integer([H1,H2], 16)) >> || <<H1,H2>> <= Hex >>.

%%
%% Decodes hexadecimal representation of object path
%% and returns list of unicode character numbers.
%%
-spec unhex_path(string()) -> list().

unhex_path(undefined) -> [];
unhex_path(Path) when erlang:is_list(Path) ->
    Bits0 = binary:split(unicode:characters_to_binary(Path), <<"/">>, [global]),

    Bits1 = [case is_valid_hex_prefix(T) of
	true -> unhex(T);
	false -> T end || T <- Bits0, erlang:byte_size(T) > 0],
    Bits2 = [unicode:characters_to_list(T) || T <- Bits1],
    join_list_with_separator(Bits2, "/", []).

%%
%% Returns UNIX timestamp by taking the number of gregorian seconds
%% and subtracting the unix time in gregorian seconds (62167219200).
%%
timestamp() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_datetime(os:timestamp()),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}}) - 62167219200.

-spec is_hidden_object(proplist()) -> boolean().

is_hidden_object(ObjInfo) ->
    ObjectKey = proplists:get_value(key, ObjInfo),
    lists:suffix(?RIAK_INDEX_FILENAME, ObjectKey) =:= true orelse 
    lists:suffix(?RIAK_ACTION_LOG_FILENAME, ObjectKey) =:= true orelse
    lists:suffix(?RIAK_LOCK_INDEX_FILENAME, ObjectKey) =:= true.

%%
%% Joins a list of elements adding a separator between each of them.
%% Example: ["a", "/", "b"]
%%
join_list_with_separator([Head|Tail], Sep, Acc0) ->
    Acc1 = case Tail of
	[] -> [Head | Acc0];  % do not add separator at the beginning
	_ -> [Sep, Head | Acc0]
    end,
    join_list_with_separator(Tail, Sep, Acc1);
join_list_with_separator([], _Sep, Acc0) -> lists:reverse(Acc0).

%%
%% Extracts token from request headers and looks it up in "security" bucket
%%
-spec check_token(any()) -> user()|undefined|not_found|expired.

check_token(Req0) ->
    case cowboy_req:header(<<"authorization">>, Req0) of
        <<"Token ", TokenValue0/binary>> ->
	    TokenValue1 = erlang:binary_to_list(TokenValue0),
	    login_handler:check_token(TokenValue1);
	_ -> undefined
    end.

%%
%% Checks if provided list has duplicate items.
%%
-spec has_duplicates(list()) -> boolean().

has_duplicates([H|T]) ->
    case lists:member(H, T) of
	true -> true;
	false -> has_duplicates(T)
    end;
has_duplicates([]) -> false.

%%
%% Returns true, if `Name` ends with `Characters`
%%
-spec ends_with(binary()|list()|undefined, binary()) -> boolean().

ends_with(undefined, _Smth) -> false;
ends_with(Name, Characters)
	when erlang:is_list(Name), erlang:is_binary(Characters) ->
    ends_with(erlang:list_to_binary(Name), Characters);
ends_with(Name, Characters)
	when erlang:is_binary(Name), erlang:is_binary(Characters) ->
    Size0 = byte_size(Characters),
    Size1 = byte_size(Name)-Size0,
    case Size1 < 0 of
	true -> false;
	false ->
	    <<_:Size1/binary, Rest/binary>> = Name,
	    Rest =:= Characters
    end.

%%
%% Returns true, if `Name` starts with `Characters`
%%
-spec starts_with(binary()|list()|undefined, binary()) -> boolean().

starts_with(undefined, _Smth) -> false;
starts_with(Name, Characters)
	when erlang:is_list(Name), erlang:is_binary(Characters) ->
    starts_with(erlang:list_to_binary(Name), Characters);
starts_with(Name, Characters)
	when erlang:is_binary(Name), erlang:is_binary(Characters) ->
    Size0 = byte_size(Characters),
    Size1 = byte_size(Name)-Size0,
    case Size1 < 0 of
	true -> false;
	false ->
	    <<Beginning:Size0/binary, _/binary>> = Name,
	    Beginning =:= Characters
    end.
