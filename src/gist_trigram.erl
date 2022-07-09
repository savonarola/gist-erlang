-module(gist_trigram).

-export([
    to_key/1
]).

-spec to_key(binary()) -> gist_key_set:key().
to_key(Str) when is_binary(Str) ->
    to_key(unicode:characters_to_list(Str), []).

to_key([Ch1, Ch2, Ch3 | Tail], Acc) ->
    to_key([Ch2, Ch3 | Tail], [list_to_atom([Ch1, Ch2, Ch3]) | Acc]);
to_key(_, Acc) ->
    maps:from_list([{K, 1} || K <- Acc]).
