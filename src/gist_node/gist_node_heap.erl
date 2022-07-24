-module(gist_node_heap).

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

init(_) ->
    #{root => undefined, level => 0}.

set_root(Data, NewRoot, NewLevel) ->
    Data#{root => NewRoot, level => NewLevel}.

get_root(#{root := Root, level := Level}) ->
    {Root, Level}.

unpack(_Data, Node) ->
    {ok, Node}.

pack_into(_Data, _PackedNode, Node) ->
    {ok, Node}.

pack(_Data, Node) ->
    Node.

delete(_Data, _PackedNode) ->
    ok.

destroy(_Data) ->
    ok.

get_values(_Data, Values) ->
    {ok, Values}.

add_value(_Data, Values, Value) ->
    {ok, [Value | Values]}.

create_leaf_node(_Data, Value) ->
    [Value].

delete_value(_Data, Values, Value) ->
    case Values -- [Value] of
        [] ->
            empty;
        NewValues ->
            {ok, NewValues}
    end.
