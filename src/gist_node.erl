-module(gist_node).

-type key() :: term().
-type packed_node() :: term().
-type packed_leaf_node() :: term().
-type data() :: term().
-type gist_node() :: [{key(), packed_node()}].
-type level() :: non_neg_integer().

-callback init(term()) -> data().
-callback get_root(data()) -> {packed_node(), level()}.
-callback set_root(data(), packed_node(), level()) -> data().
-callback unpack(data(), packed_node()) -> {ok, gist_node()} | not_found.
-callback pack_into(data(), packed_node(), gist_node()) ->
    {ok, packed_node()} | not_found.
-callback pack(data(), gist_node()) -> packed_node().
-callback delete(data(), packed_node()) -> ok | not_found.
-callback get_values(data(), packed_leaf_node()) -> {ok, [term()]} | not_found.
-callback add_value(data(), packed_leaf_node(), term()) ->
    {ok, packed_leaf_node()} | not_found.
-callback create_leaf_node(data(), term()) -> packed_leaf_node().
-callback delete_value(data(), packed_leaf_node(), term()) ->
    empty | {ok, packed_leaf_node()} | not_found.
-callback destroy(data()) -> ok.
