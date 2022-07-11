-module(gist_tree_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        % t_search_timings,
        t_insert_search
    ].

t_insert_search(_Config) ->
    MinMaxFanouts = {2, 10},
    Tree0 = gist_tree:new(gist_key_set, MinMaxFanouts),

    Words = word_list(10000),
    N = length(Words),

    {Time0, Tree1} = timer:tc(fun() ->
        lists:foldl(
            fun(Word, Tree) ->
                Key = gist_trigram:to_key(Word),
                gist_tree:insert(Tree, Key, Word)
            end,
            Tree0,
            Words
        )
    end),

    ct:print("Insert/Key: ~pms", [Time0 / N / 1_000]),
    true = erlang:garbage_collect(),

    {memory, Mem} = process_info(self(), memory),
    ct:print("Mem: ~p, Mem/Key: ~p", [Mem, Mem / N]),
    ct:print("Tree depth for ~p keys(fanouts: ~p): ~p", [N, MinMaxFanouts, gist_tree:depth(Tree1)]),

    SearchKey0 = gist_key_set:to_key(['pre', 'lly']),

    ?assertEqual(
        [<<"impressionally">>, <<"prejudicially">>],
        lists:sort(gist_tree:search(Tree1, SearchKey0))
    ),

    SearchKey1 = gist_key_set:to_key(['aaa', 'bbb']),

    ?assertEqual(
        [],
        gist_tree:search(Tree1, SearchKey1)
    ),

    AllValues = gist_tree:search(Tree1, #{}),

    ?assertEqual(lists:sort(Words), lists:sort(AllValues)).

t_search_timings(_Config) ->
    MinMaxFanouts = {2, 10},
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

    % ct:print("~s", [gist_tree:display(Tree1)]),

    ct:print("Insert/Key: ~pms", [Time0 / N / 1_000]),
    true = erlang:garbage_collect(),

    {memory, Mem} = process_info(self(), memory),
    % ct:print("Mem: ~p, Mem/Key: ~p,  Tree size: ~p, Tree flat size: ~p", [
    %     Mem, Mem / N, erts_debug:size(Tree1), erts_debug:flat_size(Tree1)
    % ]),
    ct:print("Mem: ~p, Mem/Key: ~p", [Mem, Mem / N]),
    ct:print("Tree depth for ~p keys(fanouts: ~p): ~p", [N, MinMaxFanouts, gist_tree:depth(Tree1)]),

    SampleKeys = lists:flatmap(fun(Word) -> sample_key(Word, 3) end, Words),
    ct:print("Sample key count: ~p", [length(SampleKeys)]),

    {Time1, _} =
        timer:tc(fun() ->
            search(Tree1, SampleKeys)
        end),

    ct:print("Search/Existing Key: ~pms", [Time1 / length(SampleKeys) / 1_000]),

    RandomSampleKeys = random_sample_keys(3, N),
    ct:print("Random sample key count: ~p", [N]),

    {Time2, _} =
        timer:tc(fun() ->
            search(Tree1, RandomSampleKeys)
        end),

    ct:print("Search/Random Key: ~pms", [Time2 / N / 1_000]),

    AllValues = gist_tree:search(Tree1, #{}),

    ?assertEqual(N, length(AllValues)).

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
    binary:split(Data, <<"\n">>, [global, trim_all]).

word_file(N) ->
    Dir = code:lib_dir(gist, test),
    NBin = integer_to_binary(N),
    filename:join([Dir, <<"data">>, <<"words-", NBin/binary, ".txt">>]).

random_sample_keys(N, Count) ->
    [random_key(N) || _ <- lists:seq(1, Count)].

random_key(N) when N >= 1 ->
    gist_key_set:to_key([random_trigram() || _ <- lists:seq(1, N)]).

random_trigram() ->
    TrigramString = [$a + rand:uniform(26) - 1 || _ <- lists:seq(1, 3)],
    list_to_atom(TrigramString).
