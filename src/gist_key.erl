-module(gist_key).

-type key() :: term().

-callback consistent(key(), key()) -> boolean().
-callback union(key(), key()) -> key().
-callback null_key() -> key().
-callback compress_keys([{key(), term()}]) -> [{term(), term()}].
-callback decompress_keys([{term(), term()}]) -> [{key(), term()}].
-callback penalty(key(), key()) -> number().
-callback pick_split([key()], pos_integer()) -> {[key()], [key()]}.

-optional_callbacks([display/1]).

-callback display(key()) -> iolist().
