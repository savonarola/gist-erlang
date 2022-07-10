-module(gist_tree_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        % t_insert_search,
        t_search_timings
    ].

t_insert_search(_Config) ->
    Tree0 = gist_tree:new(test, gist_key_set),

    Words = word_list(10000),

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

    ct:print("Tree tab: ~p", [ets:info(test_tree)]),
    ct:print("Data tab: ~p", [ets:info(test_data)]),
    % ct:print("~s", [gist_tree:display(Tree1)]),

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

t_search_timings(_Config) ->
    Tree0 = gist_tree:new(test, gist_key_set),

    Words = word_list(30000),

    Tree1 =
        lists:foldl(
            fun(Word, Tree) ->
                Key = gist_trigram:to_key(Word),
                gist_tree:insert(Tree, Key, Word)
            end,
            Tree0,
            Words
        ),

    SampleKeys = lists:flatmap(fun sample_key/1, Words),
    ct:print("sample key count: ~p", [length(SampleKeys)]),

    {Time, _} =
        timer:tc(fun() ->
            search(Tree1, SampleKeys)
        end),

    ct:print("search time: ~psec", [Time div 1_000_000]).

search(_Tree, []) ->
    ok;
search(Tree, [Key | Rest]) ->
    [_ | _] = gist_tree:search(Tree, Key),
    search(Tree, Rest).

%% take 2 or 1 trigrams from each word
sample_key(Word) ->
    case maps:to_list(gist_trigram:to_key(Word)) of
        [T1, T2 | _] -> [maps:from_list([T1, T2])];
        [T1 | _] -> [maps:from_list([T1])];
        _ -> []
    end.

word_list(N) ->
    {ok, Data} = file:read_file(word_file(N)),
    binary:split(Data, <<"\n">>, [global, trim_all]).

word_file(N) ->
    Dir = code:lib_dir(gist, test),
    NBin = integer_to_binary(N),
    filename:join([Dir, <<"data">>, <<"words-", NBin/binary, ".txt">>]).
