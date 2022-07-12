-module(gist_tree).

% -compile(inline_list_funcs).

-export([
    new/1,
    new/2,
    destroy/1,
    insert/3,
    search/2,
    display/1,
    depth/1
]).

-export_type([gist_tree/0]).

-type key() :: term().

-record(tree, {
    root_level = 0 :: non_neg_integer(),
    min = 2 :: pos_integer(),
    max = 20 :: pos_integer(),
    root = undefined :: [] | undefined,
    mod :: module()
}).

-type gist_tree() :: #tree{}.

-define(DISPLAY_PAD, "  ").
-define(DEFAULT_MIN_MAX_FANOUT, {2, 20}).

%%----------------------------------------------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------------------------------------------

-spec new(module()) -> gist_tree().
new(KeyMod) when is_atom(KeyMod) ->
    new(KeyMod, ?DEFAULT_MIN_MAX_FANOUT).

-spec new(module(), {pos_integer(), pos_integer()}) -> gist_tree().
new(KeyMod, {Min, Max}) when
    is_atom(KeyMod), is_integer(Min), is_integer(Max), Min > 0, Max > 0, 2 * Min =< Max
->
    #tree{
        root = undefined,
        mod = KeyMod,
        min = Min,
        max = Max
    }.

-spec destroy(gist_tree()) -> true.
destroy(#tree{}) ->
    true.

-spec depth(gist_tree()) -> non_neg_integer().
depth(#tree{root_level = L}) -> L.

-spec insert(gist_tree(), key(), term()) -> gist_tree().

%% Special case #1: empty tree
insert(#tree{root = undefined} = Tree, Key, Value) ->
    Values = add_value(Tree, undefined, Value),

    Node = [{Key, Values}],
    Tree#tree{root = Node, root_level = 0};
%% Special case #2: tree with a single node
insert(
    #tree{root = Node, root_level = 0} = Tree, NewKey, Value
) ->
    [{Key, Values}] = Node,
    case NewKey of
        Key ->
            NewValues = add_value(Tree, Values, Value),
            NewNode = [{Key, NewValues}],
            Tree#tree{root = NewNode};
        _ ->
            %% We have two different keys,
            %% so we construct a real tree with the root and two child nodes
            NewValues = add_value(Tree, undefined, Value),
            NewNode = [{NewKey, NewValues}],
            NewRootNode = [{Key, Node}, {NewKey, NewNode}],
            Tree#tree{root = NewRootNode, root_level = 1}
    end;
%% Common case, we have a tree with a root and at least two child nodes
insert(#tree{root = Node, root_level = L} = Tree, NewKey, Value) ->
    case insert(Tree, L, Node, NewKey, Value) of
        {ok, NewNode} ->
            Tree#tree{root = NewNode};
        {ok, _NewKey, NewNode} ->
            Tree#tree{root = NewNode};
        %% Need to build a new root
        {ok, NewKey1, NewNode1, NewKey2, NewNode2} ->
            NewRootNode = [{NewKey1, NewNode1}, {NewKey2, NewNode2}],
            Tree#tree{root_level = L + 1, root = NewRootNode}
    end.

search(#tree{root = Node, root_level = L} = Tree, SearchedKey) ->
    search_node(Tree, L, Node, SearchedKey).

display(#tree{root = undefined}) ->
    "[empty]\n";
display(#tree{root = Node, root_level = Level} = Tree) ->
    display_node(Tree, Level, Node).

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

%% We are at level 0 (leaf nodes), try to insert key
insert(Tree, 0, Node, NewKey, Value) ->
    case lists:keytake(NewKey, 1, Node) of
        %% The easiest case, the key exists, we just add value
        {value, {NewKey, Values}, RestChildren} ->
            NewValues = add_value(Tree, Values, Value),
            NewNode = [{NewKey, NewValues} | RestChildren],
            {ok, NewNode};
        %% New key
        false ->
            Values = add_value(Tree, undefined, Value),
            NewNode = [{NewKey, Values} | Node],
            case length(NewNode) =< Tree#tree.max of
                %% The node has space for a new key
                true ->
                    {ok, search_key(Tree, NewNode), NewNode};
                %% The node does not have space for a new key, need split
                false ->
                    {{SKey1, Children1}, {SKey2, Children2}} = split(Tree, NewNode),
                    {ok, SKey1, Children1, SKey2, Children2}
            end
    end;
%% We are at level > 0 (tree nodes), need find the best child
%% children = [] should be considered when we implement delete
insert(Tree, L, Node, NewKey, Value) when
    L > 0, Node =/= []
->
    BestKey = best_insert_key(Tree, Node, NewKey),
    {value, {BestKey, BestNode}, RestChildren} = lists:keytake(BestKey, 1, Node),
    case insert(Tree, L - 1, BestNode, NewKey, Value) of
        %% value inserted into an existing key
        {ok, NewChildNode} ->
            NewNode = [{BestKey, NewChildNode} | RestChildren],
            {ok, NewNode};
        %% value inserted into an existing node
        {ok, NewNodeKey, NewChildNode} ->
            NewNode = [{NewNodeKey, NewChildNode} | RestChildren],
            %% search_key for ADJUST_KEYS
            {ok, search_key(Tree, NewNode), NewNode};
        %% value inserted and lead to a split
        {ok, NewKey1, NewChildNode1, NewKey2, NewChildNode2} ->
            NewNode = [{NewKey1, NewChildNode1}, {NewKey2, NewChildNode2} | RestChildren],
            case length(NewNode) =< Tree#tree.max of
                %% this node has place for both split nodes
                true ->
                    %% search_key for ADJUST_KEYS
                    {ok, search_key(Tree, NewNode), NewNode};
                %% the worst case, we have no place for the both new nodes
                %% we need to split too
                false ->
                    {{SKey1, NewNode1}, {SKey2, NewNode2}} = split(Tree, NewNode),
                    {ok, SKey1, NewNode1, SKey2, NewNode2}
            end
    end.

display_node(#tree{root_level = RootLevel} = Tree, 0, Node) ->
    lists:map(
        fun({Key, Values}) ->
            [
                io_lib:format("~s[~s]~n", [pad(0, RootLevel), display_key(Tree, Key)]),
                io_lib:format("~s~p~n", [pad(0, RootLevel + 1), get_values(Tree, Values)])
            ]
        end,
        Node
    );
display_node(#tree{root_level = RootLevel} = Tree, L, Node) when L > 0 ->
    lists:map(
        fun({Key, ChildNode}) ->
            [
                io_lib:format("~s[~s]~n", [pad(L, RootLevel), display_key(Tree, Key)]),
                display_node(Tree, L - 1, ChildNode)
            ]
        end,
        Node
    ).

pad(L, RootLevel) ->
    lists:duplicate(RootLevel - L, ?DISPLAY_PAD).

search_node(#tree{mod = Mod} = Tree, 0, Node, SearchedKey) ->
    lists:concat([
        get_values(Tree, Values)
     || {Key, Values} <- Node, Mod:consistent(Key, SearchedKey)
    ]);
search_node(#tree{mod = Mod} = Tree, L, Node, SearchedKey) when
    L > 0
->
    lists:concat([
        search_node(Tree, L - 1, ChildNode, SearchedKey)
     || {Key, ChildNode} <- Node, Mod:consistent(Key, SearchedKey)
    ]).

add_value(_Tree, Values, Value) ->
    case Values of
        undefined ->
            [Value];
        _ ->
            [Value | Values]
    end.

get_values(_Tree, Values) ->
    Values.

split(#tree{min = Min, mod = Mod} = Tree, ChildrenList) ->
    Children = maps:from_list(ChildrenList),
    {Keys1, Keys2} = Mod:pick_split(maps:keys(Children), Min),
    Children1 = maps:to_list(maps:with(Keys1, Children)),
    Children2 = maps:to_list(maps:with(Keys2, Children)),
    {
        {search_key(Tree, Children1), Children1},
        {search_key(Tree, Children2), Children2}
    }.

best_insert_key(Tree, Children, NewKey) ->
    best_insert_key(Tree, Children, NewKey, undefined, undefined).

best_insert_key(_Tree, [], _NewKey, BestKey, _) ->
    BestKey;
best_insert_key(#tree{mod = Mod} = Tree, [{Key, _} | Rest], NewKey, BestKey, BestPenalty) ->
    Penalty = Mod:penalty(Key, NewKey),
    case Penalty < BestPenalty of
        true ->
            best_insert_key(Tree, Rest, NewKey, Key, Penalty);
        false ->
            best_insert_key(Tree, Rest, NewKey, BestKey, BestPenalty)
    end.

search_key(#tree{mod = Mod} = Tree, Children) ->
    search_key(Tree, Children, Mod:null_key()).

search_key(#tree{}, [], AccKey) ->
    AccKey;
search_key(#tree{mod = Mod} = Tree, [{Key, _} | Rest], AccKey) ->
    search_key(Tree, Rest, Mod:union(AccKey, Key)).

display_key(#tree{mod = Mod}, Key) ->
    try
        Mod:display(Key)
    catch
        error:undef ->
            io_lib:format("~p", [Key])
    end.
