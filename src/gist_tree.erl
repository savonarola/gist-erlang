-module(gist_tree).

% -compile(inline_list_funcs).

-export([new/3, new/4, destroy/1, insert/3, search/2, display/1, depth/1, node_data/1]).

-export_type([gist_tree/0]).

-type key() :: term().

-record(tree, {
    min = 2 :: pos_integer(),
    max = 20 :: pos_integer(),
    key_mod :: module(),
    node_mod :: module(),
    node_data = undefined
}).

-type gist_tree() :: #tree{}.

-define(DISPLAY_PAD, "  ").
-define(DEFAULT_MIN_MAX_FANOUT, {2, 20}).

%%----------------------------------------------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------------------------------------------

-spec new(module(), module(), term()) -> gist_tree().
new(KeyMod, NodeMod, NodeModOpts) when is_atom(KeyMod), is_atom(NodeMod) ->
    new(KeyMod, NodeMod, NodeModOpts, ?DEFAULT_MIN_MAX_FANOUT).

-spec new(module(), module(), term(), {pos_integer(), pos_integer()}) -> gist_tree().
new(KeyMod, NodeMod, NodeModOpts, {Min, Max}) when
    is_atom(KeyMod),
    is_integer(Min),
    is_integer(Max),
    Min > 0,
    Max > 0,
    2 * Min =< Max
->
    #tree{
        node_mod = NodeMod,
        node_data = NodeMod:init(NodeModOpts),
        key_mod = KeyMod,
        min = Min,
        max = Max
    }.

-spec destroy(gist_tree()) -> ok.
destroy(#tree{node_data = NodeData, node_mod = NodeMod}) ->
    ok = NodeMod:destroy(NodeData).

-spec depth(gist_tree()) -> non_neg_integer().
depth(Tree) ->
    {_Root, L} = get_root(Tree),
    L.

-spec insert(gist_tree(), key(), term()) -> gist_tree().
insert(#tree{node_data = NodeData, node_mod = NodeMod} = Tree, Key, Value) ->
    {Root, L} = NodeMod:get_root(NodeData),
    start_insert(Tree, Root, L, Key, Value).

-spec search(gist_tree(), key()) -> [term()].
search(Tree, SearchedKey) ->
    {RootPackedNode, L} = get_root(Tree),
    search_node(Tree, L, RootPackedNode, SearchedKey).

-spec display(gist_tree()) -> iodata().
display(Tree) ->
    case get_root(Tree) of
        {undefined, _} -> "[empty]\n";
        {RootPackedNode, Level} -> display_node(Tree, Level, Level, RootPackedNode)
    end.

%% For debug purposes only
-spec node_data(gist_tree()) -> term().
node_data(#tree{node_data = NodeData}) -> NodeData.

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

%% Special case #1: empty tree
start_insert(Tree, undefined, _Level, Key, Value) ->
    LeafPackedNode = create_leaf_node(Tree, Value),
    Node = [{Key, LeafPackedNode}],
    PackedNode = pack_node(Tree, Node),
    set_root(Tree, PackedNode, 0);
%% Special case #2: tree with a single node
start_insert(Tree, RootPackedNode, 0, NewKey, Value) ->
    {ok, [{Key, LeafPackedNode}]} = unpack_node(Tree, RootPackedNode),
    case NewKey of
        Key ->
            {ok, NewLeafPackedNode} = add_value(Tree, LeafPackedNode, Value),
            NewNode = [{Key, NewLeafPackedNode}],
            {ok, NewPackedNode} = pack_into_node(Tree, RootPackedNode, NewNode),
            set_root(Tree, NewPackedNode, 0);
        _ ->
            %% We have two different keys,
            %% so we construct a real tree with the root and two child nodes
            NewLeafPackedNode = create_leaf_node(Tree, Value),
            Node1 = [{NewKey, NewLeafPackedNode}],
            PakedNode1 = pack_node(Tree, Node1),
            Node2 = [{Key, LeafPackedNode}],
            PakedNode2 = pack_node(Tree, Node2),
            NewRootNode = [{NewKey, PakedNode1}, {Key, PakedNode2}],
            NewRootPackedNode = pack_node(Tree, NewRootNode),
            set_root(Tree, NewRootPackedNode, 1)
    end;
%% Common case, we have a tree with a root and at least two child nodes
start_insert(Tree, RootPackedNode, L, NewKey, Value) ->
    case insert(Tree, L, RootPackedNode, NewKey, Value) of
        {ok, NewNode} ->
            {ok, NewRootPackedNode} = pack_into_node(Tree, RootPackedNode, NewNode),
            set_root(Tree, NewRootPackedNode, L);
        {ok, _NewKey, NewNode} ->
            {ok, NewRootPackedNode} = pack_into_node(Tree, RootPackedNode, NewNode),
            set_root(Tree, NewRootPackedNode, L);
        %% Need to build a new root
        {ok, NewKey1, NewNode1, NewKey2, NewNode2} ->
            {ok, NewPackedNode1} = pack_into_node(Tree, RootPackedNode, NewNode1),
            NewPackedNode2 = pack_node(Tree, NewNode2),
            NewRootNode = [{NewKey1, NewPackedNode1}, {NewKey2, NewPackedNode2}],
            NewRootPackedNode = pack_node(Tree, NewRootNode),
            set_root(Tree, NewRootPackedNode, L + 1)
    end.

%% Packed nodes are passed down the recursion.
%% Unpacked nodes are returned from funs.

%% We are at level 0 (leaf nodes), try to insert key
insert(Tree, 0, PackedNode, NewKey, Value) ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    case lists:keytake(NewKey, 1, Node) of
        %% The easiest case, the key exists, we just add value
        {value, {NewKey, LeafPackedNode}, RestChildren} ->
            {ok, NewLeafPackedNode} = add_value(Tree, LeafPackedNode, Value),
            NewNode = [{NewKey, NewLeafPackedNode} | RestChildren],
            {ok, NewNode};
        %% New key
        false ->
            LeafPackedNode = create_leaf_node(Tree, Value),
            NewNode = [{NewKey, LeafPackedNode} | Node],
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
insert(Tree, L, PackedNode, NewKey, Value) when L > 0 ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    BestKey = best_insert_key(Tree, Node, NewKey),
    {value, {BestKey, BestPakedNode}, RestChildren} = lists:keytake(BestKey, 1, Node),
    case insert(Tree, L - 1, BestPakedNode, NewKey, Value) of
        %% value inserted into an existing key
        {ok, NewChildNode} ->
            {ok, NewChildPackedNode} = pack_into_node(Tree, BestPakedNode, NewChildNode),
            NewNode = [{BestKey, NewChildPackedNode} | RestChildren],
            {ok, NewNode};
        %% value inserted into an existing node
        {ok, NewNodeKey, NewChildNode} ->
            {ok, NewChildPackedNode} = pack_into_node(Tree, BestPakedNode, NewChildNode),
            NewNode = [{NewNodeKey, NewChildPackedNode} | RestChildren],
            %% search_key for ADJUST_KEYS
            {ok, search_key(Tree, NewNode), NewNode};
        %% value inserted and lead to a split
        {ok, NewKey1, NewChildNode1, NewKey2, NewChildNode2} ->
            %% use BestPakedNode for 1
            {ok, NewChildPackedNode1} = pack_into_node(Tree, BestPakedNode, NewChildNode1),
            %% create new for 2
            NewChildPackedNode2 = pack_node(Tree, NewChildNode2),
            NewNode =
                [{NewKey1, NewChildPackedNode1}, {NewKey2, NewChildPackedNode2} | RestChildren],
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

display_node(Tree, RootLevel, 0, PackedNode) ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    lists:map(
        fun({Key, LeafPackedNode}) ->
            {ok, Values} = get_values(Tree, LeafPackedNode),
            [
                io_lib:format("~s[~s]~n", [pad(0, RootLevel), display_key(Tree, Key)]),
                io_lib:format("~s~p~n", [pad(0, RootLevel + 1), Values])
            ]
        end,
        Node
    );
display_node(Tree, RootLevel, L, PackedNode) when L > 0 ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    lists:map(
        fun({Key, ChildPackedNode}) ->
            [
                io_lib:format("~s[~s]~n", [pad(L, RootLevel), display_key(Tree, Key)]),
                display_node(Tree, RootLevel, L - 1, ChildPackedNode)
            ]
        end,
        Node
    ).

-compile({inline, [unpack_node/2, pack_node/2, pack_into_node/3]}).

unpack_node(
    #tree{
        node_data = NodeData,
        node_mod = NodeMod,
        key_mod = KeyMod
    },
    PackedNode
) ->
    case NodeMod:unpack(NodeData, PackedNode) of
        not_found ->
            not_found;
        {ok, Node} ->
            {ok, KeyMod:decompress_keys(Node)}
    end.

pack_node(
    #tree{
        node_data = NodeData,
        node_mod = NodeMod,
        key_mod = KeyMod
    },
    Node
) ->
    CompressedNode = KeyMod:compress_keys(Node),
    NodeMod:pack(NodeData, CompressedNode).

pack_into_node(
    #tree{
        node_data = NodeData,
        node_mod = NodeMod,
        key_mod = KeyMod
    },
    PackedNode,
    Node
) ->
    CompressedNode = KeyMod:compress_keys(Node),
    NodeMod:pack_into(NodeData, PackedNode, CompressedNode).

pad(L, RootLevel) ->
    lists:duplicate(RootLevel - L, ?DISPLAY_PAD).

search_node(#tree{key_mod = Mod} = Tree, 0, PackedNode, SearchedKey) ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    lists:concat([
        case get_values(Tree, LeafPackedNode) of
            {ok, Values} ->
                Values;
            not_found ->
                []
        end
     || {Key, LeafPackedNode} <- Node, Mod:consistent(Key, SearchedKey)
    ]);
search_node(#tree{key_mod = Mod} = Tree, L, PackedNode, SearchedKey) when L > 0 ->
    {ok, Node} = unpack_node(Tree, PackedNode),
    lists:concat([
        search_node(Tree, L - 1, ChildPackedNode, SearchedKey)
     || {Key, ChildPackedNode} <- Node, Mod:consistent(Key, SearchedKey)
    ]).

-compile({inline, [add_value/3, get_values/2, create_leaf_node/2]}).

add_value(#tree{node_data = NodeData, node_mod = NodeMod}, LeafPackedNode, Value) ->
    NodeMod:add_value(NodeData, LeafPackedNode, Value).

get_values(#tree{node_data = NodeData, node_mod = NodeMod}, LeafPackedNode) ->
    NodeMod:get_values(NodeData, LeafPackedNode).

create_leaf_node(#tree{node_data = NodeData, node_mod = NodeMod}, Value) ->
    NodeMod:create_leaf_node(NodeData, Value).

-compile({inline, [get_root/1, set_root/3]}).

get_root(#tree{node_data = NodeData, node_mod = NodeMod}) ->
    NodeMod:get_root(NodeData).

set_root(#tree{node_data = NodeData, node_mod = NodeMod} = Tree, Root, Level) ->
    Tree#tree{node_data = NodeMod:set_root(NodeData, Root, Level)}.

split(#tree{min = Min, key_mod = Mod} = Tree, ChildrenList) ->
    Children = maps:from_list(ChildrenList),
    {Keys1, Keys2} =
        Mod:pick_split(
            maps:keys(Children), Min
        ),
    Children1 =
        maps:to_list(
            maps:with(Keys1, Children)
        ),
    Children2 =
        maps:to_list(
            maps:with(Keys2, Children)
        ),
    {{search_key(Tree, Children1), Children1}, {search_key(Tree, Children2), Children2}}.

best_insert_key(Tree, Children, NewKey) ->
    best_insert_key(Tree, Children, NewKey, undefined, undefined).

best_insert_key(_Tree, [], _NewKey, BestKey, _) ->
    BestKey;
best_insert_key(
    #tree{key_mod = Mod} = Tree,
    [{Key, _} | Rest],
    NewKey,
    BestKey,
    BestPenalty
) ->
    Penalty = Mod:penalty(Key, NewKey),
    case Penalty < BestPenalty of
        true ->
            best_insert_key(Tree, Rest, NewKey, Key, Penalty);
        false ->
            best_insert_key(Tree, Rest, NewKey, BestKey, BestPenalty)
    end.

search_key(#tree{key_mod = Mod} = Tree, Children) ->
    search_key(Tree, Children, Mod:null_key()).

search_key(#tree{}, [], AccKey) ->
    AccKey;
search_key(#tree{key_mod = Mod} = Tree, [{Key, _} | Rest], AccKey) ->
    search_key(Tree, Rest, Mod:union(AccKey, Key)).

display_key(#tree{key_mod = Mod}, Key) ->
    try
        Mod:display(Key)
    catch
        error:undef ->
            io_lib:format("~p", [Key])
    end.
