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

-record(node, {
    level = 0 :: non_neg_integer() | undefined,
    children = [] :: [{key(), reference()}]
}).

-record(tree, {
    root_level = 0 :: non_neg_integer(),
    min = 2 :: pos_integer(),
    max = 20 :: pos_integer(),
    root = undefined :: reference() | undefined,
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
    % ct:print("Empty tree"),
    Values = add_value(Tree, undefined, Value),

    Node = #node{level = 0, children = [{Key, Values}]},
    Tree#tree{root = Node, root_level = 0};
%% Special case #2: tree with a single node
insert(
    #tree{root = Node, root_level = 0} = Tree, NewKey, Value
) ->
    % ct:print("Single-node tree"),
    #node{level = 0, children = [{Key, Values}]} = Node,
    case NewKey of
        Key ->
            NewValues = add_value(Tree, Values, Value),
            NewNode = Node#node{children = [{Key, NewValues}]},
            Tree#tree{root = NewNode};
        _ ->
            %% We have two different keys,
            %% so we construct a real tree with the root and two child nodes
            NewValues = add_value(Tree, undefined, Value),
            NewNode = #node{level = 0, children = [{NewKey, NewValues}]},

            NewRootNode = #node{
                level = 1,
                children = [{Key, Node}, {NewKey, NewNode}]
            },
            Tree#tree{root = NewRootNode, root_level = 1}
    end;
%% Common case, we have a tree with a root and at least two child nodes
insert(#tree{root = Node, root_level = L} = Tree, NewKey, Value) ->
    % ct:print("General tree"),
    case insert(Tree, Node, NewKey, Value) of
        {ok, NewNode} ->
            Tree#tree{root = NewNode};
        {ok, _NewKey, NewNode} ->
            Tree#tree{root = NewNode};
        %% Need to build a new root
        {ok, NewKey1, NewNode1, NewKey2, NewNode2} ->
            NewRootChildren = [{NewKey1, NewNode1}, {NewKey2, NewNode2}],
            NewRootNode = #node{level = L + 1, children = NewRootChildren},
            Tree#tree{root_level = L + 1, root = NewRootNode}
    end.

search(#tree{root = Node} = Tree, SearchedKey) ->
    search_node(Tree, Node, SearchedKey).

display(#tree{root = undefined}) ->
    "[empty]\n";
display(#tree{root = Node, root_level = RootLevel} = Tree) ->
    display_node(Tree, Node, RootLevel).

%%----------------------------------------------------------------------------------------------------------------
%% Internal helpers
%%----------------------------------------------------------------------------------------------------------------

%% We are at level 0 (leaf nodes), try to insert key
insert(Tree, #node{level = 0, children = Children} = Node, NewKey, Value) ->
    case lists:keytake(NewKey, 1, Children) of
        %% The easiest case, the key exists, we just add value
        {value, {NewKey, Values}, RestChildren} ->
            NewValues = add_value(Tree, Values, Value),
            NewChildren = [{NewKey, NewValues} | RestChildren],
            {ok, Node#node{children = NewChildren}};
        %% New key
        false ->
            Values = add_value(Tree, undefined, Value),
            NewChildren = [{NewKey, Values} | Children],
            case length(NewChildren) =< Tree#tree.max of
                %% The node has space for a new key
                true ->
                    {ok, search_key(Tree, NewChildren), Node#node{children = NewChildren}};
                %% The node does not have space for a new key, need split
                false ->
                    {{SKey1, Children1}, {SKey2, Children2}} = split(Tree, NewChildren),
                    {ok, SKey1, #node{level = 0, children = Children1}, SKey2, #node{
                        level = 0, children = Children2
                    }}
            end
    end;
%% We are at level > 0 (tree nodes), need find the best child
%% children = [] should be considered when we implement delete
insert(Tree, #node{level = L, children = Children} = Node, NewKey, Value) when
    L > 0, Children =/= []
->
    BestKey = best_insert_key(Tree, Children, NewKey),
    {value, {BestKey, BestNode}, RestChildren} = lists:keytake(BestKey, 1, Children),
    case insert(Tree, BestNode, NewKey, Value) of
        %% value inserted into an existing key
        {ok, NewNode} ->
            NewChildren = [{BestKey, NewNode} | RestChildren],
            {ok, Node#node{children = NewChildren}};
        %% value inserted into an existing node
        {ok, NewNodeKey, NewNode} ->
            NewChildren = [{NewNodeKey, NewNode} | RestChildren],
            %% search_key for ADJUST_KEYS
            {ok, search_key(Tree, NewChildren), Node#node{children = NewChildren}};
        %% value inserted and lead to a split
        {ok, NewKey1, NewNode1, NewKey2, NewNode2} ->
            NewChildren = [{NewKey1, NewNode1}, {NewKey2, NewNode2} | RestChildren],
            case length(NewChildren) =< Tree#tree.max of
                %% this node has place for both split nodes
                true ->
                    %% search_key for ADJUST_KEYS
                    {ok, search_key(Tree, NewChildren), Node#node{children = NewChildren}};
                %% the worst case, we have no place for the both new nodes
                %% we need to split too
                false ->
                    {{SKey1, Children1}, {SKey2, Children2}} = split(Tree, NewChildren),
                    {ok, SKey1, #node{level = L, children = Children1}, SKey2, #node{
                        level = L, children = Children2
                    }}
            end
    end.

display_node(Tree, #node{level = 0, children = Children}, RootLevel) ->
    lists:map(
        fun({Key, Values}) ->
            [
                io_lib:format("~s[~s]~n", [pad(0, RootLevel), display_key(Tree, Key)]),
                io_lib:format("~s~p~n", [pad(0, RootLevel + 1), get_values(Tree, Values)])
            ]
        end,
        Children
    );
display_node(Tree, #node{level = L, children = Children}, RootLevel) when L > 0 ->
    lists:map(
        fun({Key, Node}) ->
            [
                io_lib:format("~s[~s]~n", [pad(L, RootLevel), display_key(Tree, Key)]),
                display_node(Tree, Node, RootLevel)
            ]
        end,
        Children
    ).

pad(L, RootLevel) ->
    lists:duplicate(RootLevel - L, ?DISPLAY_PAD).

search_node(#tree{mod = Mod} = Tree, #node{level = 0, children = Children}, SearchedKey) ->
    lists:concat([
        get_values(Tree, Values)
     || {Key, Values} <- Children, Mod:consistent(Key, SearchedKey)
    ]);
search_node(#tree{mod = Mod} = Tree, #node{level = L, children = Children}, SearchedKey) when
    L > 0
->
    lists:concat([
        search_node(Tree, Node, SearchedKey)
     || {Key, Node} <- Children, Mod:consistent(Key, SearchedKey)
    ]).

-compile({inline, [add_value/3]}).
add_value(_Tree, Values, Value) ->
    case Values of
        undefined ->
            [Value];
        _ ->
            [Value | Values]
    end.

-compile({inline, [get_values/2]}).
get_values(_Tree, Values) ->
    Values.

-compile({inline, [split/2]}).
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
