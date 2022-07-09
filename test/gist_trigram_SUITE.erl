-module(gist_trigram_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_to_key
    ].

t_to_key(_Config) ->
    ?assertEqual(
        #{},
        gist_trigram:to_key(<<>>)
    ),
    ?assertEqual(
        #{},
        gist_trigram:to_key(<<"a">>)
    ),
    ?assertEqual(
        #{},
        gist_trigram:to_key(<<"ab">>)
    ),
    ?assertEqual(
        #{abc => 1},
        gist_trigram:to_key(<<"abc">>)
    ),
    ?assertEqual(
        #{aba => 1, bab => 1},
        gist_trigram:to_key(<<"abababa">>)
    ).
