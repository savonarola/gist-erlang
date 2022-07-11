# GiST implementation in pure Erlang

## Description

The library implements abstract search tree according to "Generalized Search Trees for Database Systems" paper by
Joseph M. Hellerstein, Jeffrey F. Naughton, and Avi Pfeffer (Technical Report #1274, University of Wisconsin at Madison, July 1995).

Quadratic pick-split algorithm is implemented according to "R-Trees - A Dynamic Index Structure for Spatial Searching"
by Antonin Guttman (ACM SIGMOD Record, Volume 14, Issue 2, June 1984).

Currently, only `set` key class is implemented. Other key classes (like, R-trees) may be introduced by implementing `gist_key` behaviour.

Trigram search is implemented as a straightforwad demonstration of the `set` key class.

> **Warning**
>
> The library may lack many optimizations, so it may not suit for production use.

## Usage

```erlang
1> {ok, Data} = file:read_file("test/data/words-10000.txt"), Words = binary:split(Data, <<"\n">>, [global, trim_all]), length(Words).
10000
2> Tree0 = gist_tree:new(gist_key_set).
{tree,0,5,20,undefined,gist_key_set}
3> Tree1 = lists:foldl(
    fun(Word, Tree) ->
        Key = gist_trigram:to_key(Word),
        gist_tree:insert(Tree, Key, Word)
    end,
    Tree0,
    Words
), ok.
ok
4> SearchKey = gist_key_set:to_key(['and', 'nda']).
#{'and' => 1,nda => 1}
%% or:
%% SearchKey = gist_trigram:to_key(<<"anda">>).
5> gist_tree:search(Tree1, SearchKey).
[<<"demandable">>,<<"Mandan">>,<<"garlandage">>,
 <<"husbandage">>,<<"bandarlog">>]
```

## License

[Apache License](LICENSE)
