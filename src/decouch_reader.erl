-module(decouch_reader).

-include_lib("eunit/include/eunit.hrl").

-include("couch_db.hrl").

-export([open/0]).
-export([open/1]).
-export([adh/0]).
-export([all_docs/2]).
-export([all_docs_view/3]).

-export([process_docs/4, process_one/4, process_all_docs/5]).

open() ->
    open("chef_3f0cbfe0b0c0474d9ac86a8fd51d6a30.couch").

adh() ->
    {Db, _} = open(),
    all_docs(Db, couch_data),
    close(Db).

open(FilePath) ->
    DbName = "foo",
    {ok, Fd} = couch_file:open(FilePath, []),
    {ok, Header} = couch_file:read_header(Fd),
    Db = couch_db_updater:init_db(DbName, FilePath, Fd, Header),
    {Db, Header}.

close(Db) ->
                                                %    couch_file:close(Db).
    ok.

process_docs(_Db, Kv, _Reds, AccIn) ->
    ?debugVal(Kv),
    ?debugVal(Kv#full_doc_info.id),
    RevTree = Kv#full_doc_info.rev_tree,
    ?debugVal(hd(RevTree)),
    ?debugVal(couch_key_tree:get_all_leafs(RevTree)),
%%    ?debugVal(Reds),
    ?debugVal(AccIn),
    {ok, AccIn}.

process_one(Db, Kv, _Reds, AccIn) ->
    ?debugVal(Kv),
    ?debugVal(Kv#full_doc_info.id),
    RevTree = Kv#full_doc_info.rev_tree,
    ?debugVal(RevTree),
    {I, {B, X1, X2}} = hd(RevTree),
    ?debugFmt("~s", [B]),
    ?debugVal(catch binary_to_term(B)),
    ?debugVal(I), ?debugVal(B), ?debugVal(X1), ?debugVal(X2),
    ?debugVal(couch_key_tree:get_all_leafs_full(RevTree)),
    {ok, #doc{body = Body}} = couch_db:open_doc_int(Db, Kv, []),
    ?debugVal(Body),
    ?debugVal(AccIn),
    {stop, AccIn}.

process_all_docs(F, Db, Kv, _Reds, AccIn) ->
    Key = Kv#full_doc_info.id,
    {ok, #doc{body = Body}} = couch_db:open_doc_int(Db, Kv, []),
    AccOut = F(Key, Body, AccIn),
    {ok, AccOut}.

all_docs(Db, TableName) ->
    ets:new(TableName, [set,public,named_table]),
    Limit = 10,
    SkipCount = 0,
    Options = [end_key_gt], 
    FoldAccInit = {Limit, SkipCount, undefined, []},
    Fun = fun(Key, Body, AccIn) ->
                  ets:insert_new(TableName, {Key, Body}),
                  AccIn
          end,
    InFun = fun(KV, Reds, Acc) -> process_all_docs(Fun, Db, KV, Reds, Acc) end,
    Time = timer:tc(
             couch_btree, fold, [Db#db.fulldocinfo_by_id_btree, InFun, FoldAccInit, Options] ),
    ?debugVal(Time),
    ?debugVal(ets:info(TableName)).


all_docs_view(Req, Db, Keys) ->
    #view_query_args{
               start_key = StartKey,
               start_docid = StartDocId,
               end_key = EndKey,
               end_docid = EndDocId,
               limit = Limit,
               skip = SkipCount,
               direction = Dir,
               inclusive_end = Inclusive
              } = QueryArgs = couch_httpd_view:parse_view_params(Req, Keys, map),
    {ok, Info} = couch_db:get_db_info(Db),
    CurrentEtag = undef_etag,
    TotalRowCount = couch_util:get_value(doc_count, Info),
                                     StartId = if is_binary(StartKey) -> StartKey;
                                                  true -> StartDocId
                                               end,
    EndId = if is_binary(EndKey) -> EndKey;
               true -> EndDocId
            end,
    FoldAccInit = {Limit, SkipCount, undefined, []},
    UpdateSeq = couch_db:get_update_seq(Db),
%%% Keys == nil
    FoldlFun = couch_httpd_view:make_view_fold_fun(Req, QueryArgs, CurrentEtag, Db, UpdateSeq,
                                                   TotalRowCount, #view_fold_helper_funs{
                                                     reduce_count = fun couch_db:enum_docs_reduce_to_count/1
                                                    }),
    AdapterFun = fun(#full_doc_info{id=Id}=FullDocInfo, Offset, Acc) ->
                         case couch_doc:to_doc_info(FullDocInfo) of
                             #doc_info{revs=[#rev_info{deleted=false, rev=Rev}|_]} ->
                                 FoldlFun({{Id, Id}, {[{rev, couch_doc:rev_to_str(Rev)}]}}, Offset, Acc);
                             #doc_info{revs=[#rev_info{deleted=true}|_]} ->
                                 {ok, Acc}
                         end
                 end,
    {ok, LastOffset, FoldResult} =
        couch_db:enum_docs(Db,
                           AdapterFun,
                           FoldAccInit,
                           [{start_key, StartId}, {dir, Dir},
                            {if Inclusive -> end_key; true -> end_key_gt end, EndId}]),
    {LastOffset, FoldResult}.
