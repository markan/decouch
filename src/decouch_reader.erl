-module(decouch_reader).

-include_lib("eunit/include/eunit.hrl").

-include("couch_db.hrl").

-export([open/1, close/1]).
-export([open_process_all/2, process_to_ets/2]).

process_to_ets(DbName, TableName) ->
    ets:new(TableName, [set,public,named_table]),
    IterFn = fun(Key, Body, AccIn) ->
                     ets:insert_new(TableName, {Key, Body}),
                     AccIn
             end,
    open_process_all(DbName, IterFn).

open_process_all(DbName, IterFn) ->
    {Db, _} = open(DbName),
    all_docs_iter(DbName, Db, IterFn),
    close(Db).

open(FilePath) ->
    DbName = "foo",
    {ok, Fd} = couch_file:open(FilePath, []),
    {ok, Header} = couch_file:read_header(Fd),
    Db = couch_db_updater:init_db(DbName, FilePath, Fd, Header),
    {Db, Header}.

close(Db) ->
    couch_file:close(Db).

process_all_docs(F, Db, Kv, _Reds, AccIn) ->
    Key = Kv#full_doc_info.id,
    {ok, #doc{body = Body}} = couch_db:open_doc_int(Db, Kv, []),
    AccOut = F(Key, Body, AccIn),
    {ok, AccOut}.

all_docs_iter(Name, Db, IterFun) ->
    Limit = 10,
    SkipCount = 0,
    Options = [end_key_gt],
    FoldAccInit = {Limit, SkipCount, undefined, []},
    InFun = fun(KV, Reds, Acc) -> process_all_docs(IterFun, Db, KV, Reds, Acc) end,
    {Time, _} = timer:tc(
                  couch_btree, fold, [Db#db.fulldocinfo_by_id_btree, InFun, FoldAccInit, Options] ),
    io:format("Database '~s' processed in ~f seconds~n", [Name, Time/1000000]),
    ok.
