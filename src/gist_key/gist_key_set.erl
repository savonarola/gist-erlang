-module(gist_key_set).

-behaviour(gist_key).

-export([
    compress/1,
    decompress/1,
    consistent/2,
    union/1,
    penalty/2,
    pick_split/2,
    display/1
]).

-export([
    to_key/1
]).

-export_type([key/0]).

-define(MAX_DISPLAY, 100).

-type key() :: #{term() => 1}.

-spec compress(key()) -> term().
compress(Key) -> Key.

-spec decompress(term()) -> key().
decompress(Key) -> Key.

-spec consistent(key(), key()) -> boolean().
consistent(Key, QueryKey) ->
    maps:merge(Key, QueryKey) =:= Key.

-spec union([key()]) -> key().
union(Keys) ->
    lists:foldl(
        fun(Key, Acc) ->
            maps:merge(Acc, Key)
        end,
        #{},
        Keys
    ).

-spec penalty(key(), key()) -> number().
penalty(Key, KeyToAdd) ->
    maps:size(maps:merge(Key, KeyToAdd)) - maps:size(Key).

-spec pick_split([key()], pos_integer()) -> {[key()], [key()]}.
pick_split(Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    [Seed1, Seed2] = peek_seeds(Keys, Keys, undefined, undefined),
    RestKeys = Keys -- [Seed1, Seed2],
    distribute_keys(Seed1, Seed2, [Seed1], [Seed2], 1, 1, RestKeys, length(RestKeys), Min).

-spec display(key()) -> iolist().
%% For debug only
display(Key) ->
    IO = string:join(
        lists:map(
            fun(V) ->
                io_lib:format("~p", [V])
            end,
            maps:keys(Key)
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

-spec to_key(list()) -> key().
to_key(List) ->
    maps:from_list([{El, 1} || El <- List]).

distribute_keys(_Union1, _Union2, Keys1, Keys2, _Cnt1, _Cnt2, [], _CntLeft, _CntMin) ->
    {Keys1, Keys2};
distribute_keys(Union1, Union2, Keys1, Keys2, Cnt1, Cnt2, [Key | Rest] = Keys, CntLeft, CntMin) ->
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
                        union([Union1, Key]),
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
                        union([Union2, Key]),
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

peek_seeds([], [], _Score, Seeds) ->
    Seeds;
peek_seeds([_Key1 | Rest1], [], Score, Seeds) ->
    peek_seeds(Rest1, Rest1, Score, Seeds);
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
        true -> {true, NewScore};
        false -> false
    end.

score(Key1, Key2) ->
    maps:size(maps:merge(Key1, Key2)) - maps:size(Key1) - maps:size(Key2).
