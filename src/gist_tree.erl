-module(gist_tree).

-export([
    new/2,
    destroy/1,
    insert/3,
    search/2,
    display/1
]).

-export_type([gist_tree/0]).

-type key() :: term().

-record(node, {
    level = 0 :: non_neg_integer() | undefined,
    children = [] :: [{key(), reference()}]
}).

-record(tree, {
    tree_tab :: ets:table(),
    data_tab :: ets:table(),
    root_level = 0 :: non_neg_integer(),
    min = 5 :: pos_integer(),
    max = 20 :: pos_integer(),
    root = undefined :: reference() | undefined,
    mod :: module()
}).

-type gist_tree() :: #tree{}.

-define(DISPLAY_PAD, "  ").

%%----------------------------------------------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------------------------------------------

-spec new(atom(), module()) -> gist_tree().
new(Name, KeyMod) ->
    #tree{
        tree_tab = ets:new(name(Name, tree), [set, protected, named_table]),
        data_tab = ets:new(name(Name, data), [set, protected, named_table]),
        root = undefined,
        mod = KeyMod
    }.

-spec destroy(gist_tree()) -> true.
destroy(#tree{tree_tab = TTab, data_tab = DTab}) ->
    true = ets:delete(TTab),
    true = ets:delete(DTab).

-spec insert(gist_tree(), key(), term()) -> gist_tree().

%% Special case #1: empty tree
insert(#tree{root = undefined} = Tree, Key, Value) ->
    % ct:print("Empty tree"),
    ValuesId = make_ref(),
    true = add_value(Tree, ValuesId, Value),

    NodeId = make_ref(),
    Node = #node{level = 0, children = [{Key, ValuesId}]},
    true = put_node(Tree, NodeId, Node),
    Tree#tree{root = NodeId, root_level = 0};
%% Special case #2: tree with a single node
insert(
    #tree{tree_tab = TTab, root = RootId, root_level = 0} = Tree, NewKey, Value
) ->
    % ct:print("Single-node tree"),
    #node{level = 0, children = [{Key, ValuesId}]} = get_node(Tree, RootId),
    case NewKey of
        Key ->
            true = add_value(Tree, ValuesId, Value),
            Tree;
        _ ->
            %% We have two different keys,
            %% so we construct a real tree with the root and two child nodes
            NewValuesId = make_ref(),
            true = add_value(Tree, NewValuesId, Value),

            NewNodeId = make_ref(),
            NewNode = #node{level = 0, children = [{NewKey, NewValuesId}]},
            true = put_node(Tree, NewNodeId, NewNode),

            NewRootId = make_ref(),
            NewRootNode = #node{
                level = 1,
                children = [{Key, RootId}, {NewKey, NewNodeId}]
            },
            true = put_node(Tree, NewRootId, NewRootNode),
            Tree#tree{root = NewRootId, root_level = 1}
    end;
%% Common case, we have a tree with a root and at least two child nodes
insert(#tree{root = RootId, root_level = L} = Tree, NewKey, Value) ->
    % ct:print("General tree"),
    Node = get_node(Tree, RootId),
    case insert(Tree, Node, NewKey, Value) of
        ok ->
            Tree;
        {ok, {_NewKey, NewNode}} ->
            true = put_node(Tree, RootId, NewNode),
            Tree;
        %% Need to build a new root
        {ok, {NewKey1, NewNode1}, {NewKey2, NewNode2}} ->
            NodeId1 = RootId,
            true = put_node(Tree, NodeId1, NewNode1),
            NodeId2 = make_ref(),
            true = put_node(Tree, NodeId2, NewNode2),
            NewRootChildren = [{NewKey1, NodeId1}, {NewKey2, NodeId2}],
            NewRootNode = #node{level = L + 1, children = NewRootChildren},
            NewRootId = make_ref(),
            true = put_node(Tree, NewRootId, NewRootNode),
            Tree#tree{root_level = L + 1, root = NewRootId}
    end.

%% We are at level 0 (leaf nodes), try to insert key
insert(Tree, #node{level = 0, children = Children} = Node, NewKey, Value) ->
    case lists:keyfind(NewKey, 1, Children) of
        %% The easiest case, the key exists, we just add value
        {NewKey, ValuesId} ->
            true = add_value(Tree, ValuesId, Value),
            ok;
        %% New key
        false ->
            ValuesId = make_ref(),
            true = add_value(Tree, ValuesId, Value),
            NewChildren = [{NewKey, ValuesId} | Children],
            case length(NewChildren) =< Tree#tree.max of
                %% The node has space for a new key
                true ->
                    {ok, {search_key(Tree, NewChildren), Node#node{children = NewChildren}}};
                %% The node does not have space for a new key, need split
                false ->
                    {{SKey1, Children1}, {SKey2, Children2}} = split(Tree, NewChildren),
                    {ok, {SKey1, #node{level = 0, children = Children1}},
                        {SKey2, #node{level = 0, children = Children2}}}
            end
    end;
%% We are at level > 0 (tree nodes), need find the best child
%% children = [] should be considered when we implement delete
insert(Tree, #node{level = L, children = Children} = Node, NewKey, Value) when
    L > 0, Children =/= []
->
    BestKey = best_insert_key(Tree, Children, NewKey),
    {value, {BestKey, NodeId}, RestChildren} = lists:keytake(BestKey, 1, Children),
    BestNode = get_node(Tree, NodeId),
    case insert(Tree, BestNode, NewKey, Value) of
        %% value inserted into an existing key
        ok ->
            ok;
        %% value inserted into an existing node
        {ok, {NewNodeKey, NewNode}} ->
            true = put_node(Tree, NodeId, NewNode),
            NewChildren = [{NewNodeKey, NodeId} | RestChildren],
            %% search_key for ADJUST_KEYS
            {ok, {search_key(Tree, NewChildren), Node#node{children = NewChildren}}};
        %% value inserted and lead to a split
        {ok, {NewKey1, NewNode1}, {NewKey2, NewNode2}} ->
            %% save NewNode1 in place of Node
            NodeId1 = NodeId,
            true = put_node(Tree, NodeId1, NewNode1),
            NodeId2 = make_ref(),
            true = put_node(Tree, NodeId2, NewNode2),
            NewChildren = [{NewKey1, NodeId}, {NewKey2, NodeId2} | RestChildren],
            case length(NewChildren) =< Tree#tree.max of
                %% this node has place for both split nodes
                true ->
                    %% search_key for ADJUST_KEYS
                    {ok, {search_key(Tree, NewChildren), Node#node{children = NewChildren}}};
                %% the worst case, we have no place for the both new nodes
                %% we need to split too
                false ->
                    {{SKey1, Children1}, {SKey2, Children2}} = split(Tree, NewChildren),
                    {ok, {SKey1, #node{level = L, children = Children1}},
                        {SKey2, #node{level = L, children = Children2}}}
            end
    end.

search(#tree{root = RootId} = Tree, SearchedKey) ->
    search_node(Tree, get_node(Tree, RootId), SearchedKey).

display(#tree{root = undefined}) ->
    "[empty]\n";
display(#tree{root = RootId, root_level = RootLevel} = Tree) ->
    Node = get_node(Tree, RootId),
    display_node(Tree, Node, RootLevel).

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------
display_node(Tree, #node{level = 0, children = Children}, RootLevel) ->
    lists:map(
        fun({Key, ValuesId}) ->
            Values = get_values(Tree, ValuesId),
            [
                io_lib:format("~s[~s]~n", [pad(0, RootLevel), display_key(Tree, Key)]),
                io_lib:format("~s~p~n", [pad(0, RootLevel + 1), Values])
            ]
        end,
        Children
    );
display_node(Tree, #node{level = L, children = Children}, RootLevel) when L > 0 ->
    lists:map(
        fun({Key, NodeId}) ->
            [
                io_lib:format("~s[~s]~n", [pad(L, RootLevel), display_key(Tree, Key)]),
                display_node(Tree, get_node(Tree, NodeId), RootLevel)
            ]
        end,
        Children
    ).

pad(L, RootLevel) ->
    lists:duplicate(RootLevel - L, ?DISPLAY_PAD).

search_node(#tree{mod = Mod} = Tree, #node{level = 0, children = Children}, SearchedKey) ->
    lists:concat([get_values(Tree, Id) || {Key, Id} <- Children, Mod:consistent(Key, SearchedKey)]);
search_node(#tree{mod = Mod} = Tree, #node{level = L, children = Children}, SearchedKey) when
    L > 0
->
    lists:concat([
        search_node(Tree, get_node(Tree, Id), SearchedKey)
     || {Key, Id} <- Children, Mod:consistent(Key, SearchedKey)
    ]).

name(Name, Suffix) when is_atom(Name), is_atom(Suffix) ->
    list_to_atom(
        atom_to_list(Name) ++ "_" ++ atom_to_list(Suffix)
    ).

add_value(#tree{data_tab = DTab}, ValuesId, Value) ->
    case ets:lookup(DTab, ValuesId) of
        [{_, ValueSet}] ->
            ets:insert(DTab, {ValuesId, ordsets:add_element(Value, ValueSet)});
        [] ->
            ets:insert(DTab, {ValuesId, ordsets:from_list([Value])})
    end.

get_values(#tree{data_tab = DTab}, ValuesId) ->
    case ets:lookup(DTab, ValuesId) of
        [{ValuesId, ValueSet}] ->
            ordsets:to_list(ValueSet);
        [] ->
            []
    end.

put_node(#tree{tree_tab = TTab}, NodeId, Node) ->
    % ct:print("Put node ~p => ~p", [NodeId, Node]),
    ets:insert(TTab, {NodeId, Node}).

get_node(#tree{tree_tab = TTab}, NodeId) ->
    % ct:print("Get node ~p ...", [NodeId]),
    [{NodeId, Node}] = ets:lookup(TTab, NodeId),
    % ct:print("Get node ~p => ~p", [NodeId, Node]),
    Node.

split(#tree{min = Min, mod = Mod}, ChildrenList) ->
    Children = maps:from_list(ChildrenList),
    {Keys1, Keys2} = Mod:pick_split(maps:keys(Children), Min),
    {{Mod:union(Keys1), maps:to_list(maps:with(Keys1, Children))}, {
        Mod:union(Keys2), maps:to_list(maps:with(Keys2, Children))
    }}.

best_insert_key(Tree, Children, NewKey) ->
    {Keys, _} = lists:unzip(Children),
    best_insert_key(Tree, Keys, NewKey, undefined, undefined).

best_insert_key(_Tree, [], _NewKey, BestKey, _) ->
    BestKey;
best_insert_key(#tree{mod = Mod} = Tree, [Key | Rest], NewKey, BestKey, BestPenalty) ->
    Penalty = Mod:penalty(Key, NewKey),
    case Penalty < BestPenalty of
        true ->
            best_insert_key(Tree, Rest, NewKey, Key, Penalty);
        false ->
            best_insert_key(Tree, Rest, NewKey, BestKey, BestPenalty)
    end.

search_key(#tree{mod = Mod}, Children) ->
    {Keys, _} = lists:unzip(Children),
    Mod:union(Keys).

display_key(#tree{mod = Mod}, Key) ->
    try
        Mod:display(Key)
    catch
        error:undef ->
            io_lib:format("~p", [Key])
    end.
