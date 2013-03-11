%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%% ex: ts=4 sw=4 et
%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%% use this file except in compliance with the License. You may obtain a copy of
%% the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%% License for the specific language governing permissions and limitations under
%% the License.
%%-------------------------------------------------------------------
%% @author Mark Anderson <mark@opscode.com>
%% @copyright (C) 2012, Opscode Inc.
%% @doc
%%
%% @end
%% Created :  8 Nov 2012 by Mark Anderson <>
%%-------------------------------------------------------------------


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

process_each_doc(F, Db, Kv, _Reds, AccIn) ->
    Key = Kv#full_doc_info.id,
    AccOut = case couch_db:open_doc_int(Db, Kv, []) of
                 {ok, #doc{deleted=true}} ->
                     AccIn;
                 {ok, #doc{body = Body, deleted=false}} ->
                     F(Key, Body, AccIn);
                 {ok, Doc2} ->
                     ?LOG_DEBUG("process_each_doc: ~p", [Doc2]),
                     AccIn
    end,
    {ok, AccOut}.

all_docs_iter(Name, Db, IterFun) ->
    Limit = 10,
    SkipCount = 0,
    Options = [end_key_gt],
    FoldAccInit = {Limit, SkipCount, undefined, []},
    InFun = fun(KV, Reds, Acc) -> process_each_doc(IterFun, Db, KV, Reds, Acc) end,
    {Time, _} = timer:tc(
                  couch_btree, fold, [Db#db.fulldocinfo_by_id_btree, InFun, FoldAccInit, Options] ),
    ?LOG_DEBUG("Database '~s' processed in ~f seconds~n", [Name, Time/1000000]),
    ok.
