-module(utils).
-export([mime_type/1, slugify_object_name/0, slugify_object_name/1,
	 prefixed_object_name/2, increment_filename/1,
	 to_integer/1, to_integer/2, to_float/1, to_float/2,
         to_number/1, to_list/1, to_binary/1, to_atom/1, to_boolean/1,
         is_true/1, is_false/1, is_valid_bucket_name/2, is_valid_name/1,
	 hex/1, unhex/1, unhex_path/1, validate_utf8/2,
	 is_bucket_belongs_to_user/3, is_bucket_belongs_to_tenant/2,
	 get_bucket_suffix/1, is_valid_hex_prefix/1, timestamp/0,
	 is_hidden_object/1, join_list_with_separator/3]).

-include("riak.hrl").

mime_types() ->
    MimeTypesFile = "/etc/mime.types",
    {ok, MimeTypes} = httpd_conf:load_mime_types(MimeTypesFile),
    MimeTypes.

-spec mime_type(string()) -> string().

mime_type(FileName) when is_list(FileName) ->
    case filename:extension(FileName) of
	[] ->
	    "application/octet_stream";
	Extension ->
	    MimeTypes = mime_types(),
	    LookupKey = string:substr(Extension, 2),
	    proplists:get_value(LookupKey, MimeTypes, "application/octet_stream")
    end.

-spec slugify_object_name() -> string().

slugify_object_name() ->
    Length = 20,
    AllowedChars = "0123456789abcdefghijklmnopqrstuvwxyz",
    lists:foldl(fun(_, Acc) ->
	[lists:nth(rand:uniform(length(AllowedChars)),
	    AllowedChars)]
	    ++ Acc
	end, [], lists:seq(1, Length)).

-spec slugify_object_name(binary()) -> string().

slugify_object_name(FileName0) when is_binary(FileName0) ->
    FileName1 = unicode:characters_to_binary(FileName0),
    BaseName = filename:rootname(FileName1),
    Extension = filename:extension(FileName1),
    case Extension of
	[] -> slughifi:slugify(BaseName);
	_ -> lists:concat([slughifi:slugify(BaseName), slughifi:slugify(Extension)])
    end.

-spec increment_filename(binary()|string()) -> binary()|string().

increment_filename(FileName) when is_binary(FileName) ->
    {RootName, Extension} = {filename:rootname(FileName), filename:extension(FileName)},

    case binary:matches(RootName, <<"-">>) of
	[] ->
	    << RootName/binary,  <<"-1">>/binary, Extension/binary >>;
	HyphenPositions ->
	    {LastHyphenPos, _} = lists:last(HyphenPositions),
	    V = binary:part(RootName, size(RootName), -(size(RootName)-LastHyphenPos-1)),
	    try binary_to_integer(V) of
		N ->
		    NamePart = binary:part(RootName, 0, LastHyphenPos+1),
		    IncrementedValue = integer_to_binary(N+1),
		    << NamePart/binary, IncrementedValue/binary, Extension/binary >>
	    catch error:badarg ->
		<< RootName/binary,  <<"-1">>/binary, Extension/binary >>
	    end
    end;

increment_filename(FileName) when is_list(FileName) ->
    {RootName, Extension} = {filename:rootname(FileName), filename:extension(FileName)},
    LastHyphen = string:rchr(RootName, $-),
    case LastHyphen of
	0 -> string:concat(RootName ++ "-1", Extension);
	_ -> try
		N = list_to_integer(string:substr(RootName, LastHyphen+1)),
		string:concat(string:sub_string(RootName, 1, LastHyphen) ++ integer_to_list(N+1), Extension)
	     catch error:badarg ->
		string:concat(RootName ++ "-1", Extension)
	    end
    end.

%%
%% Returns 'prefix/object_name'
%% or, if prefix is empty just 'object_name'
%%
-spec prefixed_object_name(string(), string()) -> string().

prefixed_object_name(undefined, ObjectName) -> ObjectName;
prefixed_object_name([], ObjectName) -> ObjectName;
prefixed_object_name(Prefix, ObjectName0) when is_list(Prefix), is_list(ObjectName0) ->
    ObjectName1 = case string:sub_string(ObjectName0, 1, 1) =:= "/" of
	true ->
	    string:sub_string(ObjectName0, 2, length(ObjectName0));
	false ->
	    ObjectName0
    end,
    case string:sub_string(Prefix, length(Prefix), length(Prefix)) =:= "/" of
	%% strip '/' at the end, return 'prefix/object_name'
	true -> string:concat(Prefix, ObjectName1);
	_ -> string:concat(Prefix ++ "/", ObjectName1)
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

-spec to_float(string() | binary() | integer() | float(),
               strict | nonstrict) ->
                       float().
to_float(X, S) when is_binary(X) ->
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
-spec to_list(atom() | list() | binary() | integer() | float()) ->
                     list().
to_list(X)
  when erlang:is_float(X) ->
    erlang:float_to_list(X);
to_list(X)
  when erlang:is_integer(X) ->
    erlang:integer_to_list(X);
to_list(X)
  when erlang:is_binary(X) ->
    erlang:binary_to_list(X);
to_list(X)
  when erlang:is_atom(X) ->
    erlang:atom_to_list(X);
to_list(X)
  when erlang:is_list(X) ->
    X.

%% @doc
%% Known limitations:
%%   Converting [256 | _], lists with integers > 255
-spec to_binary(atom() | string() | binary() | integer() | float()) ->
                       binary().
to_binary(X)
  when erlang:is_float(X) ->
    to_binary(to_list(X));
to_binary(X)
  when erlang:is_integer(X) ->
    erlang:iolist_to_binary(integer_to_list(X));
to_binary(X)
  when erlang:is_atom(X) ->
    erlang:list_to_binary(erlang:atom_to_list(X));
to_binary(X)
  when erlang:is_list(X) ->
    erlang:iolist_to_binary(X);
to_binary(X)
  when erlang:is_binary(X) ->
    X.

-spec to_boolean(binary() | string() | atom()) ->
                        boolean().
to_boolean(<<"true">>) ->
    true;
to_boolean("true") ->
    true;
to_boolean(true) ->
    true;
to_boolean(<<"false">>) ->
    false;
to_boolean("false") ->
    false;
to_boolean(false) ->
    false.

-spec is_true(binary() | string() | atom()) ->
                     boolean().
is_true(<<"true">>) ->
    true;
is_true("true") ->
    true;
is_true(true) ->
    true;
is_true(_) ->
    false.

-spec is_false(binary() | string() | atom()) ->
                      boolean().
is_false(<<"false">>) ->
    true;
is_false("false") ->
    true;
is_false(false) ->
    true;
is_false(_) ->
    false.

%% @doc
%% Automation conversion a term to an existing atom. badarg is
%% returned if the atom doesn't exist.  the safer version, won't let
%% you leak atoms
-spec to_atom(atom() | list() | binary() | integer() | float()) ->
                     atom().
to_atom(X)
  when erlang:is_atom(X) ->
    X;
to_atom(X)
  when erlang:is_list(X) ->
    erlang:list_to_existing_atom(X);
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
%% - Suffix is either "private" or "public"
%%
-spec is_valid_bucket_name(string(), string()) -> boolean().

is_valid_bucket_name(BucketName, TenantName)
	when erlang:is_list(BucketName), erlang:is_list(TenantName) ->
    Bits = string:tokens(BucketName, "-"),
    case length(Bits) =:= 4 andalso length(BucketName) =< 63 of
	true ->
	    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
	    BucketSuffix = lists:last(Bits),
	    BucketSuffix = lists:last(Bits),
	    (BucketSuffix =:= ?PRIVATE_BUCKET_SUFFIX
	     orelse BucketSuffix =:= ?PUBLIC_BUCKET_SUFFIX
	    ) andalso BucketTenantName =:= TenantName
	      andalso lists:prefix([?RIAK_BACKEND_PREFIX], Bits) =:= true;
	false ->
	    false
    end;
is_valid_bucket_name(_, _) -> false.

even(X) when X >= 0 -> (X band 1) == 0.

%%
%% Checks if provided prefix consists of allowed characters
%%
is_valid_hex_prefix(HexPrefix) when erlang:is_binary(HexPrefix) ->
    F0 = fun(T) ->
	    case even(erlang:byte_size(T)) of
		true ->
		    case validate_hex(T, 0) of
			0 -> is_valid_name(unhex(T));
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
%% Returns "private" or "public"
%%
-spec get_bucket_suffix(string()) -> string().

get_bucket_suffix(BucketName) when erlang:is_list(BucketName) ->
    Bits = string:tokens(BucketName, "-"),
    lists:last(Bits).

%%
%% Returns true, if "tenant id", encoded in BucketName
%% equals to provided TenantName. Also it checks if "user id",
%% equals to provided UserId.
%% the-companyname-gesfecso-public
%% ^^^ ^^^^^^^^^^ ^^^
%% prefix  bucket  username
%%
-spec is_bucket_belongs_to_user(string(), string(), string()) -> boolean().

is_bucket_belongs_to_user(BucketName, UserName, TenantName)
    when erlang:is_list(BucketName), erlang:is_list(UserName),
	 erlang:is_list(TenantName) ->
    Bits = string:tokens(BucketName, "-"),
    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
    BucketUserName = string:to_lower(lists:nth(3, Bits)),
    BucketUserName =:= UserName andalso BucketTenantName =:= TenantName;
is_bucket_belongs_to_user(_,_,_) -> false.

is_bucket_belongs_to_tenant(BucketName, TenantName)
    when erlang:is_list(BucketName), erlang:is_list(TenantName) ->
    Bits = string:tokens(BucketName, "-"),
    BucketTenantName = string:to_lower(lists:nth(2, Bits)),
    BucketTenantName =:= TenantName;
is_bucket_belongs_to_tenant(_,_) -> false.

%% This function returns 0 when binary string do not contain forbidden 
%% characters. It returns 1 otherwise.
%% FORBIDDEN_CHARS are ' " ` < >
is_valid_name(<<>>, State) -> State;
is_valid_name(<< $', _/bits >>, 0) -> 1;
is_valid_name(<< $", _/bits >>, 0) -> 1;
is_valid_name(<< $`, _/bits >>, 0) -> 1;
is_valid_name(<< $<, _/bits >>, 0) -> 1;
is_valid_name(<< $>, _/bits >>, 0) -> 1;
is_valid_name(<< _, Rest/bits >>, 0) -> is_valid_name(Rest, 0).

is_valid_name(Prefix) when erlang:is_binary(Prefix) ->
    (size(Prefix) =< 254) andalso (validate_utf8(Prefix, 0) =:= 0) andalso (is_valid_name(Prefix, 0) =:= 0);
is_valid_name(_) ->
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

hex(undefined) -> <<>>;
hex(Bin) -> << << (digit(A1)),(digit(A2)) >> || <<A1:4,A2:4>> <= Bin >>.
unhex(Hex) -> << << (erlang:list_to_integer([H1,H2], 16)) >> || <<H1,H2>> <= Hex >>.

%%
%% Decodes hexadecimal representation of object path
%% and returns list of unicode character numbers.
%%
-spec unhex_path(binary()|string()) -> list().

unhex_path(Path) when erlang:is_list(Path) orelse erlang:is_list(Path) ->
    Bits0 = binary:split(to_binary(Path), <<"/">>, [global]),

    Bits1 = [case is_valid_hex_prefix(T) of
	true -> unhex(T);
	false -> T end || T <- Bits0, erlang:byte_size(T) > 0],
    Bits2 = [unicode:characters_to_list(T) || T <- Bits1],
    lists:flatten(join_list_with_separator(Bits2, "/", [])).

timestamp() ->
    {{Year, Month, Day}, {Hour, Minute, Second}} = calendar:now_to_datetime(os:timestamp()),
    calendar:datetime_to_gregorian_seconds({{Year, Month, Day}, {Hour, Minute, Second}}) - 62167219200.

-spec is_hidden_object(proplist()) -> boolean().

is_hidden_object(ObjInfo) ->
    ObjectName = proplists:get_value(key, ObjInfo),
    lists:suffix(?RIAK_INDEX_FILENAME, ObjectName) == true orelse 
	lists:suffix(?RIAK_ACTION_LOG_FILENAME, ObjectName) == true.

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
