-module(gist_key_set_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    [
        t_union,
        t_consistent,
        t_compress,
        t_decompress,
        t_penalty,
        t_pick_split
    ].

t_union(_Config) ->
    ?assertEqual(
        #{a => 1, b => 1, c => 1},
        gist_key_set:union(
            [#{a => 1}, #{b => 1}, #{c => 1}]
        )
    ),

    ?assertEqual(
        #{a => 1, b => 1, c => 1},
        gist_key_set:union(
            [#{a => 1, b => 1}, #{b => 1, c => 1}, #{c => 1, a => 1}]
        )
    ).

t_consistent(_Config) ->
    ?assert(
        gist_key_set:consistent(
            #{a => 1, b => 1},
            #{a => 1}
        )
    ),

    ?assertNot(
        gist_key_set:consistent(
            #{a => 1, b => 1},
            #{a => 1, c => 1}
        )
    ).

t_compress(_Config) ->
    ?assertEqual(
        #{a => 1, b => 1, c => 1},
        gist_key_set:compress(
            #{a => 1, b => 1, c => 1}
        )
    ).

t_decompress(_Config) ->
    ?assertEqual(
        #{a => 1, b => 1, c => 1},
        gist_key_set:decompress(
            #{a => 1, b => 1, c => 1}
        )
    ).

t_penalty(_Config) ->
    ?assertEqual(
        0,
        gist_key_set:penalty(
            #{a => 1, b => 1, c => 1}, #{a => 1, b => 1, c => 1}
        )
    ),

    ?assertEqual(
        0,
        gist_key_set:penalty(
            #{a => 1, b => 1, c => 1}, #{a => 1}
        )
    ),

    ?assertEqual(
        2,
        gist_key_set:penalty(
            #{a => 1}, #{a => 1, b => 1, c => 1}
        )
    ).

t_pick_split(_Config) ->
    {Keys1, Keys2} = gist_key_set:pick_split(
        [
            #{a => 1},
            #{d => 1},
            #{a => 1, b => 1},
            #{d => 1, e => 1},
            #{a => 1, b => 1, c => 1},
            #{d => 1, e => 1, f => 1}
        ],
        2
    ),

    ?assertEqual(
        [
            [#{a => 1}, #{a => 1, b => 1}, #{a => 1, b => 1, c => 1}],
            [#{d => 1}, #{d => 1, e => 1}, #{d => 1, e => 1, f => 1}]
        ],
        lists:sort([lists:sort(Keys1), lists:sort(Keys2)])
    ).
