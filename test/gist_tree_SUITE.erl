-module(gist_tree_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_create
    ].

t_create(_Config) ->
    Tree0 = gist_tree:new(test, gist_key_set),

    Words = word_list(),

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

    ct:print("Time: ~psec", [Time0 div 1_000_000]),

    % ct:print("Tree tab: ~p", [ets:info(test_tree)]),
    % ct:print("Data tab: ~p", [ets:info(test_data)]),
    % ct:print("~s", [gist_tree:display(Tree1)])

    SearchKey = gist_key_set:to_key(['and', 'nda']),

    ?assertEqual(
        [
            <<"Mandan">>,
            <<"bandarlog">>,
            <<"demandable">>,
            <<"garlandage">>,
            <<"husbandage">>
        ],
        lists:sort(gist_tree:search(Tree1, SearchKey))
    ).

word_list() ->
    {ok, Data} = file:read_file(word_file()),
    binary:split(Data, <<"\n">>, [global, trim_all]).

word_file() ->
    Dir = code:lib_dir(gist, test),
    filename:join([Dir, <<"data">>, <<"words.txt">>]).
