-module(gist_tree_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(MIN_MAX_FANOUT, {2, 10}).

all() ->
    [
        % t_insert_search_set_heap,
        % t_insert_search_set_ets,
        t_insert_search_hash_heap
        % t_concurrent_read
    ].

init_per_testcase(t_insert_search_set_heap, Config) ->
    Tree = gist_tree:new(gist_key_set, undefined, gist_node_heap, undefined, ?MIN_MAX_FANOUT),
    [{tree, Tree} | Config];
init_per_testcase(t_insert_search_set_ets, Config) ->
    Tree = gist_tree:new(gist_key_set, undefined, gist_node_ets, undefined, ?MIN_MAX_FANOUT),
    [{tree, Tree} | Config];
init_per_testcase(t_insert_search_hash_heap, Config) ->
    Tree = gist_tree:new(gist_key_mhash, {64, 3}, gist_node_heap, undefined, ?MIN_MAX_FANOUT),
    [{tree, Tree} | Config];
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_insert_search_set_heap, Config) ->
    ok = gist_tree:destroy(?config(tree, Config));
end_per_testcase(t_insert_search_set_ets, Config) ->
    ok = gist_tree:destroy(?config(tree, Config));
end_per_testcase(t_insert_search_hash_heap, Config) ->
    ok = gist_tree:destroy(?config(tree, Config));
end_per_testcase(_, Config) ->
    Config.

t_insert_search_set_heap(Config) ->
    t_insert_search(?config(tree, Config)).

t_insert_search_set_ets(Config) ->
    t_insert_search(?config(tree, Config)).

t_insert_search_hash_heap(Config) ->
    t_insert_search(?config(tree, Config)).

t_insert_search(Tree0) ->
    Words = word_list(30000),
    Topics = zip_shuffled(Words, 3),
    N = length(Words),

    {Time0, Tree1} =
        timer:tc(fun() ->
            lists:foldl(
                fun(Topic, Tree) ->
                    Key = gist_tree:to_key(Tree, Topic),
                    gist_tree:insert(Tree, Key, Topic)
                end,
                Tree0,
                Topics
            )
        end),

    ct:print("Insert/Key: ~pms", [Time0 / N / 1_000]),
    ct:print(
        "Tree depth for ~p keys(fanouts: ~p): ~p",
        [N, ?MIN_MAX_FANOUT, gist_tree:depth(Tree1)]
    ),

    % SearchKey0 = gist_tree:to_key(Tree1, [<<"impressionally">>]),
    SearchKey0 = gist_tree:to_key(Tree1, [<<"disguisement">>]),

    {Time1, SearchRes} = timer:tc(fun() ->
        gist_tree:search(Tree1, SearchKey0)
    end),

    ct:print("Search key: ~p~nTime: ~p~nresult: ~p", [SearchKey0, Time1, SearchRes]),

    % ?assert(lists:member(<<"impressionally">>, SearchRes)),
    % ?assert(lists:member(<<"prejudicially">>, SearchRes)),

    % SearchKey1 = gist_tree:to_key(Tree1, [aaa, bbb]),

    % ?assertEqual([], gist_tree:search(Tree1, SearchKey1)),

    AllValues = gist_tree:search(Tree1, gist_tree:null_key(Tree1)),

    ct:print("All values length: ~p", [length(AllValues)]),

    ?assertEqual(lists:sort(Topics), lists:sort(AllValues)).

t_concurrent_read(_Config) ->
    Tree0 = gist_tree:new(gist_key_set, undefined, gist_node_ets, undefined, {8, 20}),

    TestWord = list_to_binary(lists:seq($a, $n)),
    TestKey = gist_tree:to_key(Tree0, gist_trigram:trigram_atoms(TestWord)),
    Tree1 = gist_tree:insert(Tree0, TestKey, TestWord),

    SearcherPid = spawn_link(
        fun() ->
            search_loop(1, Tree1, TestWord, TestKey)
        end
    ),

    Words = word_list(10000),
    _Tree2 =
        lists:foldl(
            fun(Word, Tree) ->
                Key = gist_tree:to_key(Tree, gist_trigram:trigram_atoms(Word)),
                gist_tree:insert(Tree, Key, Word)
            end,
            Tree1,
            Words
        ),
    ct:sleep(2000),

    SearcherPid ! done,

    ?assert(
        lists:member(
            TestWord,
            gist_tree:search(Tree1, TestKey)
        )
    ).

search_loop(N, Tree, TestWord, TestKey) ->
    ?assert(
        lists:member(
            TestWord,
            gist_tree:search(Tree, TestKey)
        )
    ),
    N rem 100 == 0 andalso ct:print("Successful searches: ~p", [N]),
    receive
        done -> ok
    after 0 ->
        search_loop(N + 1, Tree, TestWord, TestKey)
    end.

word_list(N) ->
    {ok, Data} = file:read_file(word_file(N)),
    binary:split(Data, <<"\n">>, [global, trim_all]).

word_file(N) ->
    Dir = code:lib_dir(gist, test),
    NBin = integer_to_binary(N),
    filename:join([Dir, <<"data">>, <<"words-", NBin/binary, ".txt">>]).

shuffle(List) ->
    {_, ShuffledList} = lists:unzip(lists:sort([{rand:uniform(), X} || X <- List])),
    ShuffledList.

zip_shuffled(List, 1) ->
    [[X] || X <- shuffle(List)];
zip_shuffled(List, N) ->
    lists:zipwith(
        fun(X, Zipped) ->
            [X | Zipped]
        end,
        shuffle(List),
        zip_shuffled(List, N - 1)
    ).

