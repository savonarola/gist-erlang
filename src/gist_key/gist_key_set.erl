-module(gist_key_set).

-behaviour(gist_key).

% -compile(inline_list_funcs).

-export([
    init/1,
    compress_keys/2,
    decompress_keys/2,
    consistent/3,
    union/3,
    null_key/1,
    penalty/3,
    pick_split/3,
    seed_score/3,
    display/2
]).
-export([to_key/2]).

-export_type([key/0]).

-define(MAX_DISPLAY, 100).

-type key() :: #{term() => 1}.
-type data() :: undefined.

%%----------------------------------------------------------------------------------------------------------------
%% gist_key behaviour
%%----------------------------------------------------------------------------------------------------------------

-spec init(any()) -> data().
init(_) -> undefined.

-spec compress_keys(data(), [{key(), term()}]) -> [{term(), term()}].
compress_keys(_, KV) -> KV.

-spec decompress_keys(data(), [{term(), term()}]) -> [{key(), term()}].
decompress_keys(_, KV) -> KV.

-spec consistent(data(), key(), key()) -> boolean().
consistent(_, Key, QueryKey) ->
    maps:merge(Key, QueryKey) =:= Key.

-spec union(data(), key(), key()) -> key().
union(_, Key1, Key2) ->
    maps:merge(Key1, Key2).

-spec null_key(data()) -> key().
null_key(_) ->
    #{}.

-spec penalty(data(), key(), key()) -> number().
penalty(_, Key, KeyToAdd) ->
    do_penalty(Key, maps:keys(KeyToAdd), 0).

-spec display(data(), key()) -> iolist().
%% For debug only
display(_, Key) ->
    IO = string:join(
        lists:map(
            fun(V) -> io_lib:format("~p", [V]) end,
            lists:sort(
                maps:keys(Key)
            )
        ),
        ","
    ),
    S = binary_to_list(iolist_to_binary(IO)),
    case string:length(S) > ?MAX_DISPLAY of
        true ->
            string:sub_string(S, 1, ?MAX_DISPLAY - 3) ++ "...";
        false ->
            S
    end.

-spec seed_score(data(), key(), key()) -> number().
seed_score(_, Key1, Key2) ->
    maps:size(
        maps:merge(Key1, Key2)
    ) -
        maps:size(Key1) -
        maps:size(Key2).

-spec pick_split(data(), [key()], pos_integer()) -> {[key()], [key()]}.
pick_split(Data, Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    gist_key:pick_split(?MODULE, Data, Keys, Min).

%%----------------------------------------------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------------------------------------------

-spec to_key(data(), list()) -> key().
to_key(_, List) ->
    maps:from_list([{El, 1} || El <- List]).

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

do_penalty(_Key, [], Score) ->
    Score;
do_penalty(Key, [KTAKey | Rest], Score) ->
    case Key of
        #{KTAKey := _} ->
            do_penalty(Key, Rest, Score);
        _ ->
            do_penalty(Key, Rest, Score + 1)
    end.
