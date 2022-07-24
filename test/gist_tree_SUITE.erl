-module(gist_tree_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(MIN_MAX_FANOUT, {2, 10}).

all() ->
    [
        t_insert_search_heap,
        t_insert_search_ets
    ].

init_per_testcase(t_insert_search_heap, Config) ->
    Tree = gist_tree:new(gist_key_set, gist_node_heap, undefined, ?MIN_MAX_FANOUT),
    [{tree, Tree} | Config];
init_per_testcase(t_insert_search_ets, Config) ->
    Tree = gist_tree:new(gist_key_set, gist_node_ets, undefined, ?MIN_MAX_FANOUT),
    [{tree, Tree} | Config];
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(t_insert_search_heap, Config) ->
    ok = gist_tree:destroy(?config(tree, Config));
end_per_testcase(t_insert_search_ets, Config) ->
    ok = gist_tree:destroy(?config(tree, Config));
end_per_testcase(_, Config) ->
    Config.

t_insert_search_heap(Config) ->
    t_insert_search(?config(tree, Config)).

t_insert_search_ets(Config) ->
    t_insert_search(?config(tree, Config)).

t_insert_search(Tree0) ->
    Words = word_list(10000),
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

    ct:print("Insert/Key: ~pms", [Time0 / N / 1_000]),
    ct:print(
        "Tree depth for ~p keys(fanouts: ~p): ~p",
        [N, ?MIN_MAX_FANOUT, gist_tree:depth(Tree1)]
    ),

    SearchKey0 = gist_key_set:to_key([pre, lly]),

    ?assertEqual(
        [<<"impressionally">>, <<"prejudicially">>],
        lists:sort(
            gist_tree:search(Tree1, SearchKey0)
        )
    ),

    SearchKey1 = gist_key_set:to_key([aaa, bbb]),

    ?assertEqual([], gist_tree:search(Tree1, SearchKey1)),

    AllValues = gist_tree:search(Tree1, gist_key_set:null_key()),

    ?assertEqual(lists:sort(Words), lists:sort(AllValues)).

word_list(N) ->
    {ok, Data} = file:read_file(word_file(N)),
    binary:split(Data, <<"\n">>, [global, trim_all]).

word_file(N) ->
    Dir = code:lib_dir(gist, test),
    NBin = integer_to_binary(N),
    filename:join([Dir, <<"data">>, <<"words-", NBin/binary, ".txt">>]).
