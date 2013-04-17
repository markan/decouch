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

-export([
         close/1,
         open/1,
         open_process_all/2,
         open_process_all/3,
         process_to_ets/2
        ]).

process_to_ets(DbName, TableName) ->
    ets:new(TableName, [set,public,named_table]),
    IterFn = fun(Key, Body, AccIn) ->
                     ets:insert_new(TableName, {Key, Body}),
                     AccIn
             end,
    open_process_all(DbName, IterFn, []).

%% @doc Given the path to a couchdb data file `DbName', open the database and call `IterFn'
%% on each non-deleted document. The fun `IterFn' should take four arguments: `Key',
%% `RevId', `Body', `Acc'. The accumulated result is returned.
open_process_all(DbName, IterFn, Acc) ->
    {Db, _} = open(DbName),
    Result = all_docs_iter(DbName, Db, IterFn, Acc),
    close(Db),
    Result.

%% @doc Same as {@link open_process_all/3} but called for side-effect. The `Acc' is
%% initialized with `[]'. This is here to keep backwards compat of the API.
open_process_all(DbName, IterFn) ->
    open_process_all(DbName, IterFn, []).

open(FilePath) ->
    DbName = filename:basename(FilePath),
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
                 {ok, #doc{body = Body, deleted=false} = Doc} ->
                     RevId = get_rev_id(Doc),
                     {A, B, C, IterAcc} = AccIn,
                     IterAccOut = F(Key, RevId, Body, IterAcc),
                     {A, B, C, IterAccOut}
    end,
    {ok, AccOut}.

all_docs_iter(Name, Db, IterFun, IterFunAcc) ->
    %% what's the limit 10 do?
    Limit = 10,
    SkipCount = 0,
    Options = [end_key_gt],
    FoldAccInit = {Limit, SkipCount, undefined, IterFunAcc},
    InFun = fun(KV, Reds, Acc) -> process_each_doc(IterFun, Db, KV, Reds, Acc) end,
    {Time, {ok, _, Result}} = timer:tc(fun() ->
                                               couch_btree:fold(Db#db.fulldocinfo_by_id_btree,
                                                                InFun, FoldAccInit, Options)
                                       end),
    ?LOG_DEBUG("Database '~s' processed in ~f seconds~n", [Name, Time/1.0e6]),
    {_, _, _, IterResult} = Result,
    IterResult.

get_rev_id(#doc{revs = {Count, [Rev|_]}}) ->
    iolist_to_binary([integer_to_list(Count), <<"-">>, hexiolist(Rev)]).

%% we could also use couch_util:to_hex/1. In some informal testing, this is ~60% faster.
hexiolist(<<X:128/big-unsigned-integer>>) ->
    io_lib:format("~32.16.0b", [X]).
