-module(gist_key_hash).

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

-type key() :: #{term() => 1}.
-type data() :: undefined.

%%----------------------------------------------------------------------------------------------------------------
%% gist_key behaviour
%%----------------------------------------------------------------------------------------------------------------

-spec init(pos_integer()) -> data().
init(N) when is_integer(N), N > 0 -> N * 8.

-spec compress_keys(data(), [{key(), term()}]) -> [{term(), term()}].
compress_keys(_, KV) -> KV.

-spec decompress_keys(data(), [{term(), term()}]) -> [{key(), term()}].
decompress_keys(_, KV) -> KV.

-spec consistent(data(), key(), key()) -> boolean().
consistent(_Bits, Key, QueryKey) ->
    Key bor QueryKey =:= Key.

-spec union(data(), key(), key()) -> key().
union(_Bits, Key1, Key2) ->
    Key1 bor Key2.

-spec null_key(data()) -> key().
null_key(_Bits) ->
    0.

-spec penalty(data(), key(), key()) -> number().
penalty(_Bits, Key, KeyToAdd) ->
    bit_sum(Key bor KeyToAdd) - bit_sum(Key).

-spec display(data(), key()) -> iolist().
%% For debug only
display(_Bits, Key) ->
    io_lib:format("~.2B", [Key]).

-spec seed_score(data(), key(), key()) -> number().
seed_score(_Bits, Key1, Key2) ->
    bit_sum(Key1 bor Key2) - bit_sum(Key1) - bit_sum(Key2).

-spec pick_split(data(), [key()], pos_integer()) -> {[key()], [key()]}.
pick_split(Bits, Keys, Min) when length(Keys) >= Min * 2, Min > 0 ->
    gist_key:pick_split(?MODULE, Bits, Keys, Min).

-spec to_key(data(), list()) -> key().
to_key(Bits, List) ->
    lists:foldl(
        fun(El, Acc) ->
            Acc bor (1 bsl erlang:phash2(El, Bits))
        end,
        0,
        List
    ).

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
