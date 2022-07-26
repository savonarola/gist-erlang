-module(gist_node_ets).

-behaviour(gist_node).

-export([
    init/1,
    set_root/3,
    get_root/1,
    unpack/2,
    pack_into/3,
    pack/2,
    delete/2,
    destroy/1,
    get_values/2,
    add_value/3,
    create_leaf_node/2,
    delete_value/3
]).

-record(st, {leaf_tab = undefined :: ets:table(), node_tab = undefined :: ets:table()}).

init(_) ->
    LeafTab = ets:new(gist_node_ets_values, [bag, protected]),
    NodeTab = ets:new(gist_node_ets_nodes, [set, protected]),
    #st{leaf_tab = LeafTab, node_tab = NodeTab}.

set_root(#st{node_tab = NodeTab} = Data, NewRoot, NewLevel) ->
    true = ets:insert(NodeTab, {root, {NewRoot, NewLevel}}),
    Data.

get_root(#st{node_tab = NodeTab}) ->
    case ets:lookup(NodeTab, root) of
        [{root, {Root, Level}}] -> {Root, Level};
        [] -> {undefined, 0}
    end.

unpack(#st{node_tab = NodeTab}, NodeRef) ->
    case ets:lookup(NodeTab, NodeRef) of
        [{NodeRef, Node}] ->
            {ok, Node};
        [] ->
            not_found
    end.

pack_into(#st{node_tab = NodeTab}, NodeRef, Node) ->
    true = ets:insert(NodeTab, {NodeRef, Node}),
    {ok, NodeRef}.

pack(#st{node_tab = NodeTab}, Node) ->
    NodeRef = make_ref(),
    true = ets:insert(NodeTab, {NodeRef, Node}),
    NodeRef.

delete(#st{node_tab = NodeTab}, NodeRef) ->
    true = ets:delete(NodeTab, NodeRef),
    ok.

destroy(#st{node_tab = NodeTab, leaf_tab = LeafTab}) ->
    true = ets:delete(NodeTab),
    true = ets:delete(LeafTab),
    ok.

get_values(#st{leaf_tab = LeafTab}, LeafRef) ->
    case ets:lookup(LeafTab, LeafRef) of
        [] ->
            not_found;
        KVs ->
            {_, Values} = lists:unzip(KVs),
            {ok, Values}
    end.

add_value(#st{leaf_tab = LeafTab}, LeafRef, Value) ->
    true = ets:insert(LeafTab, {LeafRef, Value}),
    {ok, LeafRef}.

create_leaf_node(#st{leaf_tab = LeafTab}, Value) ->
    LeafRef = make_ref(),
    true = ets:insert(LeafTab, {LeafRef, Value}),
    LeafRef.

delete_value(#st{leaf_tab = LeafTab}, LeafRef, Value) ->
    true = ets:delete_object(LeafTab, {LeafRef, Value}),
    case ets:lookup(LeafTab, LeafRef) of
        [] ->
            empty;
        _ ->
            {ok, LeafRef}
    end.
