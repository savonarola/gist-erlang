-module(gist_key).

-type key() :: term().
-type st() :: term().

-export([pick_split/4]).

-export_type([key/0]).

-callback init(term()) -> st().

-callback consistent(st(), key(), key()) -> boolean().
-callback union(st(), key(), key()) -> key().
-callback null_key(st()) -> key().
-callback compress_keys(st(), [{key(), term()}]) -> [{term(), term()}].
-callback decompress_keys(st(), [{term(), term()}]) -> [{key(), term()}].
-callback penalty(st(), key(), key()) -> number().
-callback seed_score(st(), key(), key()) -> number().
-callback pick_split(st(), [key()], pos_integer()) -> {[key()], [key()]}.
-callback to_key(st(), term()) -> key().

-optional_callbacks([display/2]).

-callback display(st(), key()) -> iolist().

-spec pick_split(module(), st(), [key()], pos_integer()) -> {[key()], [key()]}.
pick_split(Mod, St, Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    [Seed1, Seed2] = peek_seeds(Mod, St, Keys),
    RestKeys = Keys -- [Seed1, Seed2],
    distribute_keys(Mod, St, Seed1, Seed2, [Seed1], [Seed2], 1, 1, RestKeys, length(RestKeys), Min).

distribute_keys(_Mod, _St, _Union1, _Union2, Keys1, Keys2, _Cnt1, _Cnt2, [], _CntLeft, _CntMin) ->
    {Keys1, Keys2};
distribute_keys(
    Mod,
    St,
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
            Penalty1 = Mod:penalty(St, Union1, Key),
            Penalty2 = Mod:penalty(St, Union2, Key),
            case Penalty1 < Penalty2 of
                true ->
                    %% Better to add to Union1
                    distribute_keys(
                        Mod,
                        St,
                        Mod:union(St, Union1, Key),
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
                        Mod,
                        St,
                        Union1,
                        Mod:union(St, Union2, Key),
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

peek_seeds(Mod, St, Keys) when length(Keys) > 1 ->
    peek_seeds(Mod, St, Keys, tl(Keys), undefined, undefined).

peek_seeds(_Mod, _St, [_], [], _Score, Seeds) ->
    Seeds;
peek_seeds(Mod, St, [_Key1 | Rest1], [], Score, Seeds) ->
    peek_seeds(Mod, St, Rest1, tl(Rest1), Score, Seeds);
peek_seeds(Mod, St, [Key1 | Rest1], [Key2 | Rest2], Score, Seeds) ->
    case are_better_seeds(Mod, St, Key1, Key2, Score) of
        {true, NewScore} ->
            peek_seeds(Mod, St, [Key1 | Rest1], Rest2, NewScore, [Key1, Key2]);
        false ->
            peek_seeds(Mod, St, [Key1 | Rest1], Rest2, Score, Seeds)
    end.

are_better_seeds(Mod, St, Key1, Key2, undefined) ->
    {true, Mod:seed_score(St, Key1, Key2)};
are_better_seeds(Mod, St, Key1, Key2, Score) ->
    NewScore = Mod:seed_score(St, Key1, Key2),
    case NewScore > Score of
        true ->
            {true, NewScore};
        false ->
            false
    end.
