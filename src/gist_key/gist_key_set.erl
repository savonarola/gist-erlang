-module(gist_key_set).

-behaviour(gist_key).

% -compile(inline_list_funcs).

-export([
    compress_keys/1,
    decompress_keys/1,
    consistent/2,
    union/2,
    null_key/0,
    penalty/2,
    pick_split/2,
    display/1
]).
-export([to_key/1]).

-export_type([key/0]).

-define(MAX_DISPLAY, 100).

-type key() :: #{term() => 1}.

%%----------------------------------------------------------------------------------------------------------------
%% gist_key behaviour
%%----------------------------------------------------------------------------------------------------------------

-spec compress_keys([{key(), term()}]) -> [{term(), term()}].
compress_keys(KV) -> KV.

-spec decompress_keys([{term(), term()}]) -> [{key(), term()}].
decompress_keys(KV) -> KV.

-spec consistent(key(), key()) -> boolean().
consistent(Key, QueryKey) ->
    maps:merge(Key, QueryKey) =:= Key.

-spec union(key(), key()) -> key().
union(Key1, Key2) ->
    maps:merge(Key1, Key2).

-spec null_key() -> key().
null_key() ->
    #{}.

-spec penalty(key(), key()) -> number().
penalty(Key, KeyToAdd) ->
    penalty(Key, maps:keys(KeyToAdd), 0).

-spec pick_split([key()], pos_integer()) -> {[key()], [key()]}.
pick_split(Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    [Seed1, Seed2] = peek_seeds(Keys),
    RestKeys = Keys -- [Seed1, Seed2],
    distribute_keys(Seed1, Seed2, [Seed1], [Seed2], 1, 1, RestKeys, length(RestKeys), Min).

-spec display(key()) -> iolist().
%% For debug only
display(Key) ->
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

%%----------------------------------------------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------------------------------------------

-spec to_key(list()) -> key().
to_key(List) ->
    maps:from_list([{El, 1} || El <- List]).

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

penalty(_Key, [], Score) ->
    Score;
penalty(Key, [KTAKey | Rest], Score) ->
    case Key of
        #{KTAKey := _} ->
            penalty(Key, Rest, Score);
        _ ->
            penalty(Key, Rest, Score + 1)
    end.

distribute_keys(_Union1, _Union2, Keys1, Keys2, _Cnt1, _Cnt2, [], _CntLeft, _CntMin) ->
    {Keys1, Keys2};
distribute_keys(
    Union1,
    Union2,
    Keys1,
    Keys2,
    Cnt1,
    Cnt2,
    [Key | Rest] = Keys,
    CntLeft,
    CntMin
) ->
    if
        Cnt1 + CntLeft == CntMin ->
            %% All go to Union1
            {Keys1 ++ Keys, Keys2};
        Cnt2 + CntLeft == CntMin ->
            %% All go to Union2
            {Keys1, Keys2 ++ Keys};
        true ->
            %% Have to select which group is better
            Penalty1 = penalty(Union1, Key),
            Penalty2 = penalty(Union2, Key),
            case Penalty1 < Penalty2 of
                true ->
                    %% Better to add to Union1
                    distribute_keys(
                        union(Union1, Key),
                        Union2,
                        [Key | Keys1],
                        Keys2,
                        Cnt1 + 1,
                        Cnt2,
                        Rest,
                        CntLeft - 1,
                        CntMin
                    );
                false ->
                    %% Better to add to Union2
                    distribute_keys(
                        Union1,
                        union(Union2, Key),
                        Keys1,
                        [Key | Keys2],
                        Cnt1,
                        Cnt2 + 1,
                        Rest,
                        CntLeft - 1,
                        CntMin
                    )
            end
    end.

peek_seeds(Keys) when length(Keys) > 1 ->
    peek_seeds(Keys, tl(Keys), undefined, undefined).

peek_seeds([_], [], _Score, Seeds) ->
    Seeds;
peek_seeds([_Key1 | Rest1], [], Score, Seeds) ->
    peek_seeds(Rest1, tl(Rest1), Score, Seeds);
peek_seeds([Key1 | Rest1], [Key2 | Rest2], Score, Seeds) ->
    case are_better_seeds(Key1, Key2, Score) of
        {true, NewScore} ->
            peek_seeds([Key1 | Rest1], Rest2, NewScore, [Key1, Key2]);
        false ->
            peek_seeds([Key1 | Rest1], Rest2, Score, Seeds)
    end.

are_better_seeds(Key1, Key2, undefined) ->
    {true, score(Key1, Key2)};
are_better_seeds(Key1, Key2, Score) ->
    NewScore = score(Key1, Key2),
    case NewScore > Score of
        true ->
            {true, NewScore};
        false ->
            false
    end.

score(Key1, Key2) ->
    maps:size(
        maps:merge(Key1, Key2)
    ) -
        maps:size(Key1) -
        maps:size(Key2).
