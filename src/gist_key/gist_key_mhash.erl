-module(gist_key_mhash).

-behaviour(gist_key).

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

-type key() :: {integer(), reference() | undefined}.

-record(st, {
    bits :: pos_integer(),
    n :: pos_integer()
}).

-type data() :: #st{}.

%%----------------------------------------------------------------------------------------------------------------
%% gist_key behaviour
%%----------------------------------------------------------------------------------------------------------------

-spec init(pos_integer()) -> data().
init({NBytes, NHashes}) when is_integer(NBytes), NBytes > 0 -> #st{bits = NBytes * 8, n = NHashes}.

-spec compress_keys(data(), [{key(), term()}]) -> [{term(), term()}].
compress_keys(_St, KV) -> KV.

-spec decompress_keys(data(), [{term(), term()}]) -> [{key(), term()}].
decompress_keys(_St, KV) -> KV.

-spec consistent(data(), key(), key()) -> boolean().
consistent(_St, {Key, _Ref}, {QueryKey, _QueryRef}) ->
    Key bor QueryKey =:= Key.

-spec union(data(), key(), key()) -> key().
union(_St, {Key1, _Ref1}, {Key2, _Ref2}) ->
    {Key1 bor Key2, make_ref()}.

-spec null_key(data()) -> key().
null_key(_St) ->
    {0, undefined}.

-spec penalty(data(), key(), key()) -> number().
penalty(_St, {Key, _Ref}, {KeyToAdd, _RefToAdd}) ->
    case bit_sum(Key bor KeyToAdd) - bit_sum(Key) of
        0 ->
            - bit_sum(Key);
        N ->
            N
    end.

-spec display(data(), key()) -> iolist().
%% For debug only
display(_St, {Key, _Ref}) ->
    io_lib:format("~.2B", [Key]).

-spec seed_score(data(), key(), key()) -> number().
seed_score(_St, {Key1, _Ref1}, {Key2, _Ref2}) ->
    bit_sum(Key1 bor Key2) - bit_sum(Key1) - bit_sum(Key2).

-spec pick_split(data(), [key()], pos_integer()) -> {[key()], [key()]}.
pick_split(St, Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    gist_key:pick_split(?MODULE, St, Keys, Min).

-spec to_key(data(), list()) -> key().
to_key(#st{bits = NBits, n = NHashs}, List) ->
    {Key, _} = lists:foldl(
        fun(El0, {Acc0, Index}) ->
            Els = [{HI, Index, El0} || HI <- lists:seq(1, NHashs)],
            Acc0New = lists:foldl(
                fun(El, Acc1) ->
                    Acc1 bor (1 bsl erlang:phash2(El, NBits))
                end,
                Acc0,
                Els
            ),
            {Acc0New, Index + 1}
        end,
        {0, 0},
        List
    ),
    {Key, undefined}.

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

bit_sum(N) when N >= 2#1111 ->
    bit_sum(N, 0);
bit_sum(2#0000) ->
    0;
bit_sum(2#0001) ->
    1;
bit_sum(2#0010) ->
    1;
bit_sum(2#0011) ->
    2;
bit_sum(2#0100) ->
    1;
bit_sum(2#0101) ->
    2;
bit_sum(2#0110) ->
    2;
bit_sum(2#0111) ->
    3;
bit_sum(2#1000) ->
    1;
bit_sum(2#1001) ->
    2;
bit_sum(2#1010) ->
    2;
bit_sum(2#1011) ->
    3;
bit_sum(2#1100) ->
    2;
bit_sum(2#1101) ->
    3;
bit_sum(2#1110) ->
    3;
bit_sum(2#1111) ->
    4.

bit_sum(0, Acc) -> Acc;
bit_sum(N, Acc) -> bit_sum(N div 16#F, Acc + bit_sum(N rem 16#F)).
