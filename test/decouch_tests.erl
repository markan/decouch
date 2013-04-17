-module(decouch_tests).

-compile([export_all]).

-include_lib("eunit/include/eunit.hrl").

-define(DB_FILE, "../test/test_db.couch").

-define(DOC_3, {<<"doc-3">>,
                <<"1-9cffaefa1f31754b956e321b5b792f9d">>,
                {[{<<"name">>,<<"sam-3">>},
                  {<<"count">>,3}]}}).

open_process_all_test() ->
    Data = ets:new(decouch_test, [set]),
    IterFun = fun(Key, RevId, Body, N) ->
                      ets:insert(Data, {Key, RevId, Body}),
                      N + 1
              end,
    Result = decouch_reader:open_process_all(?DB_FILE, IterFun, 0),

    ?assertEqual(1000, Result),

    %% test_db.couch contains 1000 simple docs
    ?assertEqual(1000, proplists:get_value(size, ets:info(Data))),

    ?assertEqual(?DOC_3, hd(ets:lookup(Data, <<"doc-3">>))),


    ?assertEqual(<<"1-5e1d515218481dafda3424134a5d3f1b">>,
                 get_rev_id(Data, <<"doc-201">>)),

    %% doc-201 was deleted and re-added
    ?assertEqual(<<"1-5e1d515218481dafda3424134a5d3f1b">>,
                 get_rev_id(Data, <<"doc-201">>)),

    %% doc-100 was updated three times
    ?assertEqual(<<"3-f66a4cb3f6820ec77d8c63ee442de426">>,
                 get_rev_id(Data, <<"doc-100">>)),

    %% doc-200 was deleted
    ?assertEqual([], ets:lookup(Data, <<"doc-200">>)).

get_rev_id(Data, Key) ->
    [{Key, RevId, _}] = ets:lookup(Data, Key),
    RevId.

-ifdef(PERF_TEST).
bench_test() ->
    IterFun = fun(_Key, <<"1-", Rev/binary>>, _Body, Acc) ->
                      [hexstr_to_bin(Rev) | Acc];
                 (_, _, _, Acc) ->
                      Acc
              end,
    Revs = decouch_reader:open_process_all(?DB_FILE, IterFun, []),

    {T1, _} = timer:tc(fun() ->
                               FF = fun() ->
                                            [ couch_util:to_hex(X) || X <- Revs ]
                                    end,
                               loop(FF, 5)
                       end),
    ?debugVal({to_hex, T1}),
    {T2, _} = timer:tc(fun() ->
                               FF = fun() ->
                                            [ hexiolist(X) || X <- Revs ]
                                    end,
                               loop(FF, 5)
                       end),
    ?debugVal({hexiolist, T2}),
    ok.

hexstr_to_bin(B) when is_binary(B) ->
    hexstr_to_bin(binary_to_list(B));
hexstr_to_bin(S) ->
  hexstr_to_bin(S, []).
hexstr_to_bin([], Acc) ->
  list_to_binary(lists:reverse(Acc));
hexstr_to_bin([X,Y|T], Acc) ->
  {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
  hexstr_to_bin(T, [V | Acc]).

hexiolist(<<X:128/big-unsigned-integer>>) ->
    io_lib:format("~32.16.0b", [X]).

loop(_, 0) ->
    ok;
loop(F, N) ->
    F(),
    loop(F, N - 1).

-endif.
