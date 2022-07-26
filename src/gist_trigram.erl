-module(gist_trigram).

-export([trigram_atoms/1]).

-spec trigram_atoms(binary()) -> gist_key_set:key().
trigram_atoms(Str) when is_binary(Str) ->
    trigram_atoms(unicode:characters_to_list(Str), []).

trigram_atoms([Ch1, Ch2, Ch3 | Tail], Acc) ->
    trigram_atoms([Ch2, Ch3 | Tail], [list_to_atom([Ch1, Ch2, Ch3]) | Acc]);
trigram_atoms(_, Acc) ->
    Acc.
