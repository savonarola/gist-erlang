-module(gist_key).

-type key() :: term().

-callback consistent(key(), key()) -> boolean().
-callback union([key()]) -> key().
-callback compress(key()) -> term().
-callback decompress(term()) -> key().
-callback penalty(key(), key()) -> number().
-callback pick_split([key()], pos_integer()) -> {[key()], [key()]}.

-optional_callbacks([display/1]).
-callback display(key()) -> iolist().
