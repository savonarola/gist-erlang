-module(gist_sample).

-export([
    test_search/0,
    test_insert/0,
    test_timings/0,
    insert/2,
    search/2
]).

test_search() ->
    MinMaxFanouts = {2, 10},
    Tree0 = gist_tree:new(gist_key_set, MinMaxFanouts),

    Words = word_list(10000),
    Tree1 =
        lists:foldl(
            fun(Word, Tree) ->
                Key = gist_trigram:to_key(Word),
                gist_tree:insert(Tree, Key, Word)
            end,
            Tree0,
            Words
        ),

    true = erlang:garbage_collect(),

    SampleKeys = lists:flatmap(fun(Word) -> sample_key(Word, 3) end, Words),

    fprof:apply(?MODULE, search, [Tree1, SampleKeys]),
    fprof:profile(),
    fprof:analyse([no_callers, {cols, 120}, {sort, acc}]).

test_insert() ->
    MinMaxFanouts = {2, 5},
    Tree0 = gist_tree:new(gist_key_set, MinMaxFanouts),

    Words = word_list(10000),
    fprof:apply(?MODULE, insert, [Tree0, Words]),
    fprof:profile(),
    fprof:analyse([no_callers, {cols, 120}, {sort, acc}]).

test_timings() ->
    MinMaxFanouts = {2, 4},
    Tree0 = gist_tree:new(gist_key_set, MinMaxFanouts),

    Words = word_list(100000),
    N = length(Words),

    {Time0, Tree1} =
        timer:tc(fun() ->
            lists:foldl(
                fun(Word, Tree) ->
                    Key = gist_trigram:to_key(Word),
                    gist_tree:insert(Tree, Key, Word)
                end,
                Tree0,
                Words
            )
        end),

    io:format("~s~n", [gist_tree:display(Tree1)]),

    io:format("Insert/Key: ~pms~n", [Time0 / N / 1_000]),
    true = erlang:garbage_collect(),

    {memory, Mem} = process_info(self(), memory),
    % io:format("Mem: ~p, Mem/Key: ~p,  Tree size: ~p, Tree flat size: ~p", [
    %     Mem, Mem / N, erts_debug:size(Tree1), erts_debug:flat_size(Tree1)
    % ]),
    io:format("Mem: ~p, Mem/Key: ~p~n", [Mem, Mem / N]),
    io:format("Tree depth for ~p keys(fanouts: ~p): ~p~n", [
        N, MinMaxFanouts, gist_tree:depth(Tree1)
    ]),

    SampleKeys = lists:flatmap(fun(Word) -> sample_key(Word, 3) end, Words),
    io:format("Sample key count: ~p~n", [length(SampleKeys)]),

    {Time1, _} =
        timer:tc(fun() ->
            search(Tree1, SampleKeys)
        end),

    io:format("Search/Existing Key: ~pms~n", [Time1 / length(SampleKeys) / 1_000]),

    RandomSampleKeys = random_sample_keys(3, N),
    io:format("Random sample key count: ~p~n", [N]),

    {Time2, _} =
        timer:tc(fun() ->
            search(Tree1, RandomSampleKeys)
        end),

    io:format("Search/Random Key: ~pms~n", [Time2 / N / 1_000]).

insert(Tree, Words) ->
    lists:foldl(
        fun(Word, T) ->
            Key = gist_trigram:to_key(Word),
            gist_tree:insert(T, Key, Word)
        end,
        Tree,
        Words
    ).

search(_Tree, []) ->
    ok;
search(Tree, [Key | Rest]) ->
    _ = gist_tree:search(Tree, Key),
    search(Tree, Rest).

%% take N trigrams from each word
sample_key(Word, N) ->
    Trigrams = gist_trigram:to_key(Word),
    case maps:size(Trigrams) >= N of
        true -> [maps:from_list(lists:sublist(maps:to_list(Trigrams), N))];
        false -> []
    end.

word_list(N) ->
    {ok, Data} = file:read_file(word_file(N)),
    lists:map(
        fun string:lowercase/1,
        binary:split(Data, <<"\n">>, [global, trim_all])
    ).

word_file(N) ->
    NBin = integer_to_binary(N),
    filename:join([<<"test">>, <<"data">>, <<"words-", NBin/binary, ".txt">>]).

random_sample_keys(N, Count) ->
    [random_key(N) || _ <- lists:seq(1, Count)].

random_key(N) when N >= 1 ->
    gist_key_set:to_key([random_trigram() || _ <- lists:seq(1, N)]).

random_trigram() ->
    TrigramString = [$a + rand:uniform(26) - 1 || _ <- lists:seq(1, 3)],
    list_to_atom(TrigramString).
