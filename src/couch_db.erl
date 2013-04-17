% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_db).

-export([get_db_info/1,get_design_docs/1]).
-export([count_changes_since/2]).
-export([get_doc_info/2,open_doc/2,open_doc/3,open_doc_revs/4]).
-export([get_revs_limit/1]).
-export([get_missing_revs/2,name/1,doc_to_tree/1,get_update_seq/1,get_committed_update_seq/1]).
-export([enum_docs/4,enum_docs_since/5]).
-export([enum_docs_since_reduce_to_count/1,enum_docs_reduce_to_count/1]).
-export([get_purge_seq/1,get_last_purged/1]).
-export([open_doc_int/3]).
-export([read_doc/2,new_revid/1]).
-export([check_is_admin/1, check_is_reader/1]).

-export([open_db_file/2]).

-include("couch_db.hrl").



open_db_file(Filepath, Options) ->
    case couch_file:open(Filepath, Options) of
    {ok, Fd} ->
        {ok, Fd};
    {error, enoent} ->
        % couldn't find file. is there a compact version? This can happen if
        % crashed during the file switch.
        case couch_file:open(Filepath ++ ".compact") of
        {ok, Fd} ->
            ?LOG_INFO("Found ~s~s compaction file, using as primary storage.", [Filepath, ".compact"]),
            ok = file:rename(Filepath ++ ".compact", Filepath),
            ok = couch_file:sync(Fd),
            {ok, Fd};
        {error, enoent} ->
            {not_found, no_db_file}
        end;
    Error ->
        Error
    end.

open_doc(Db, IdOrDocInfo) ->
    open_doc(Db, IdOrDocInfo, []).

open_doc(Db, Id, Options) ->
    case open_doc_int(Db, Id, Options) of
    {ok, #doc{deleted=true}=Doc} ->
        case lists:member(deleted, Options) of
        true ->
            apply_open_options({ok, Doc},Options);
        false ->
            {not_found, deleted}
        end;
    Else ->
        apply_open_options(Else,Options)
    end.

apply_open_options({ok, Doc},Options) ->
    apply_open_options2(Doc,Options);
apply_open_options(Else,_Options) ->
    Else.
    
apply_open_options2(Doc,[]) ->
    {ok, Doc};
apply_open_options2(#doc{atts=Atts,revs=Revs}=Doc,
        [{atts_since, PossibleAncestors}|Rest]) ->
    RevPos = find_ancestor_rev_pos(Revs, PossibleAncestors),
    apply_open_options2(Doc#doc{atts=[A#att{data=
        if AttPos>RevPos -> Data; true -> stub end}
        || #att{revpos=AttPos,data=Data}=A <- Atts]}, Rest);
apply_open_options2(Doc,[_|Rest]) ->
    apply_open_options2(Doc,Rest).


find_ancestor_rev_pos({_, []}, _AttsSinceRevs) ->
    0;
find_ancestor_rev_pos(_DocRevs, []) ->
    0;
find_ancestor_rev_pos({RevPos, [RevId|Rest]}, AttsSinceRevs) ->
    case lists:member({RevPos, RevId}, AttsSinceRevs) of
    true ->
        RevPos;
    false ->
        find_ancestor_rev_pos({RevPos - 1, Rest}, AttsSinceRevs)
    end.

open_doc_revs(Db, Id, Revs, Options) ->
    [{ok, Results}] = open_doc_revs_int(Db, [{Id, Revs}], Options),
    {ok, [apply_open_options(Result, Options) || Result <- Results]}.

% Each returned result is a list of tuples:
% {Id, MissingRevs, PossibleAncestors}
% if no revs are missing, it's omitted from the results.
get_missing_revs(Db, IdRevsList) ->
    Results = get_full_doc_infos(Db, [Id1 || {Id1, _Revs} <- IdRevsList]),
    {ok, find_missing(IdRevsList, Results)}.

find_missing([], []) ->
    [];
find_missing([{Id, Revs}|RestIdRevs], [{ok, FullInfo} | RestLookupInfo]) ->
    case couch_key_tree:find_missing(FullInfo#full_doc_info.rev_tree, Revs) of
    [] ->
        find_missing(RestIdRevs, RestLookupInfo);
    MissingRevs ->
        #doc_info{revs=RevsInfo} = couch_doc:to_doc_info(FullInfo),
        LeafRevs = [Rev || #rev_info{rev=Rev} <- RevsInfo],
        % Find the revs that are possible parents of this rev
        PossibleAncestors =
        lists:foldl(fun({LeafPos, LeafRevId}, Acc) ->
            % this leaf is a "possible ancenstor" of the missing
            % revs if this LeafPos lessthan any of the missing revs
            case lists:any(fun({MissingPos, _}) ->
                    LeafPos < MissingPos end, MissingRevs) of
            true ->
                [{LeafPos, LeafRevId} | Acc];
            false ->
                Acc
            end
        end, [], LeafRevs),
        [{Id, MissingRevs, PossibleAncestors} |
                find_missing(RestIdRevs, RestLookupInfo)]
    end;
find_missing([{Id, Revs}|RestIdRevs], [not_found | RestLookupInfo]) ->
    [{Id, Revs, []} | find_missing(RestIdRevs, RestLookupInfo)].

get_doc_info(Db, Id) ->
    case get_full_doc_info(Db, Id) of
    {ok, DocInfo} ->
        {ok, couch_doc:to_doc_info(DocInfo)};
    Else ->
        Else
    end.

%   returns {ok, DocInfo} or not_found
get_full_doc_info(Db, Id) ->
    [Result] = get_full_doc_infos(Db, [Id]),
    Result.

get_full_doc_infos(Db, Ids) ->
    couch_btree:lookup(Db#db.fulldocinfo_by_id_btree, Ids).

get_committed_update_seq(#db{committed_update_seq=Seq}) ->
    Seq.

get_update_seq(#db{update_seq=Seq})->
    Seq.

get_purge_seq(#db{header=#db_header{purge_seq=PurgeSeq}})->
    PurgeSeq.

get_last_purged(#db{header=#db_header{purged_docs=nil}}) ->
    {ok, []};
get_last_purged(#db{fd=Fd, header=#db_header{purged_docs=PurgedPointer}}) ->
    couch_file:pread_term(Fd, PurgedPointer).

get_db_info(Db) ->
    #db{fd=Fd,
        header=#db_header{disk_version=DiskVersion},
        compactor_pid=Compactor,
        update_seq=SeqNum,
        name=Name,
        fulldocinfo_by_id_btree=FullDocBtree,
        instance_start_time=StartTime,
        committed_update_seq=CommittedUpdateSeq,
	compact_seq=CompactSeq} = Db,
    {ok, Size} = couch_file:bytes(Fd),
    {ok, {Count, DelCount}} = couch_btree:full_reduce(FullDocBtree),
    InfoList = [
        {db_name, Name},
        {doc_count, Count},
        {doc_del_count, DelCount},
        {update_seq, SeqNum},
        {purge_seq, couch_db:get_purge_seq(Db)},
        {compact_running, Compactor/=nil},
	{compact_seq, CompactSeq},
        {disk_size, Size},
        {instance_start_time, StartTime},
        {disk_format_version, DiskVersion},
        {committed_update_seq, CommittedUpdateSeq}],
    {ok, InfoList}.

get_design_docs(#db{fulldocinfo_by_id_btree=Btree}=Db) ->
    {ok,_, Docs} = couch_btree:fold(Btree,
        fun(#full_doc_info{id= <<"_design/",_/binary>>}=FullDocInfo, _Reds, AccDocs) ->
            {ok, Doc} = couch_db:open_doc_int(Db, FullDocInfo, []),
            {ok, [Doc | AccDocs]};
        (_, _Reds, AccDocs) ->
            {stop, AccDocs}
        end,
        [], [{start_key, <<"_design/">>}, {end_key_gt, <<"_design0">>}]),
    {ok, Docs}.

check_is_admin(#db{user_ctx=#user_ctx{name=Name,roles=Roles}}=Db) ->
    {Admins} = get_admins(Db),
    AdminRoles = [<<"_admin">> | couch_util:get_value(<<"roles">>, Admins, [])],
    AdminNames = couch_util:get_value(<<"names">>, Admins,[]),
    case AdminRoles -- Roles of
    AdminRoles -> % same list, not an admin role
        case AdminNames -- [Name] of
        AdminNames -> % same names, not an admin
            throw({unauthorized, <<"You are not a db or server admin.">>});
        _ ->
            ok
        end;
    _ ->
        ok
    end.

check_is_reader(#db{user_ctx=#user_ctx{name=Name,roles=Roles}=UserCtx}=Db) ->
    case (catch check_is_admin(Db)) of
    ok -> ok;
    _ ->
        {Readers} = get_readers(Db),
        ReaderRoles = couch_util:get_value(<<"roles">>, Readers,[]),
        WithAdminRoles = [<<"_admin">> | ReaderRoles],
        ReaderNames = couch_util:get_value(<<"names">>, Readers,[]),
        case ReaderRoles ++ ReaderNames of
        [] -> ok; % no readers == public access
        _Else ->
            case WithAdminRoles -- Roles of
            WithAdminRoles -> % same list, not an reader role
                case ReaderNames -- [Name] of
                ReaderNames -> % same names, not a reader
                    ?LOG_DEBUG("Not a reader: UserCtx ~p vs Names ~p Roles ~p",[UserCtx, ReaderNames, WithAdminRoles]),
                    throw({unauthorized, <<"You are not authorized to access this db.">>});
                _ ->
                    ok
                end;
            _ ->
                ok
            end
        end
    end.

get_admins(#db{security=SecProps}) ->
    couch_util:get_value(<<"admins">>, SecProps, {[]}).

get_readers(#db{security=SecProps}) ->
    couch_util:get_value(<<"readers">>, SecProps, {[]}).

get_revs_limit(#db{revs_limit=Limit}) ->
    Limit.

name(#db{name=Name}) ->
    Name.

new_revid(#doc{body=Body,revs={OldStart,OldRevs},
        atts=Atts,deleted=Deleted}) ->
    case [{N, T, M} || #att{name=N,type=T,md5=M} <- Atts, M =/= <<>>] of
    Atts2 when length(Atts) =/= length(Atts2) ->
        % We must have old style non-md5 attachments
        ?l2b(integer_to_list(couch_util:rand32()));
    Atts2 ->
        OldRev = case OldRevs of [] -> 0; [OldRev0|_] -> OldRev0 end,
        couch_util:md5(term_to_binary([Deleted, OldStart, OldRev, Body, Atts2]))
    end.

enum_docs_since_reduce_to_count(Reds) ->
    couch_btree:final_reduce(
            fun couch_db_updater:btree_by_seq_reduce/2, Reds).

enum_docs_reduce_to_count(Reds) ->
    {Count, _DelCount} = couch_btree:final_reduce(
            fun couch_db_updater:btree_by_id_reduce/2, Reds),
    Count.

count_changes_since(Db, SinceSeq) ->
    {ok, Changes} =
    couch_btree:fold_reduce(Db#db.docinfo_by_seq_btree,
        fun(_SeqStart, PartialReds, 0) ->
            {ok, couch_btree:final_reduce(Db#db.docinfo_by_seq_btree, PartialReds)}
        end,
        0, [{start_key, SinceSeq + 1}]),
    Changes.

enum_docs_since(Db, SinceSeq, InFun, Acc, Options) ->
    {ok, LastReduction, AccOut} = couch_btree:fold(Db#db.docinfo_by_seq_btree, InFun, Acc, [{start_key, SinceSeq + 1} | Options]),
    {ok, enum_docs_since_reduce_to_count(LastReduction), AccOut}.

enum_docs(Db, InFun, InAcc, Options) ->
    {ok, LastReduce, OutAcc} = couch_btree:fold(Db#db.fulldocinfo_by_id_btree, InFun, InAcc, Options),
    {ok, enum_docs_reduce_to_count(LastReduce), OutAcc}.

%%% Internal function %%%
open_doc_revs_int(Db, IdRevs, Options) ->
    Ids = [Id || {Id, _Revs} <- IdRevs],
    LookupResults = get_full_doc_infos(Db, Ids),
    lists:zipwith(
        fun({Id, Revs}, Lookup) ->
            case Lookup of
            {ok, #full_doc_info{rev_tree=RevTree}} ->
                {FoundRevs, MissingRevs} =
                case Revs of
                all ->
                    {couch_key_tree:get_all_leafs(RevTree), []};
                _ ->
                    case lists:member(latest, Options) of
                    true ->
                        couch_key_tree:get_key_leafs(RevTree, Revs);
                    false ->
                        couch_key_tree:get(RevTree, Revs)
                    end
                end,
                FoundResults =
                lists:map(fun({Value, {Pos, [Rev|_]}=FoundRevPath}) ->
                    case Value of
                    ?REV_MISSING ->
                        % we have the rev in our list but know nothing about it
                        {{not_found, missing}, {Pos, Rev}};
                    {IsDeleted, SummaryPtr, _UpdateSeq} ->
                        {ok, make_doc(Db, Id, IsDeleted, SummaryPtr, FoundRevPath)}
                    end
                end, FoundRevs),
                Results = FoundResults ++ [{{not_found, missing}, MissingRev} || MissingRev <- MissingRevs],
                {ok, Results};
            not_found when Revs == all ->
                {ok, []};
            not_found ->
                {ok, [{{not_found, missing}, Rev} || Rev <- Revs]}
            end
        end,
        IdRevs, LookupResults).

open_doc_int(Db, <<?LOCAL_DOC_PREFIX, _/binary>> = Id, _Options) ->
    %% local docs are docs tagged not to be replicated. They have a
    %% magic prefix that prevents them from being forwarded over
    %% replication.
    case couch_btree:lookup(Db#db.local_docs_btree, [Id]) of
    [{ok, {_, {Rev, BodyData}}}] ->
        {ok, #doc{id=Id, revs={0, [list_to_binary(integer_to_list(Rev))]}, body=BodyData}};
    [not_found] ->
        {not_found, missing}
    end;
open_doc_int(Db, #doc_info{id=Id,revs=[RevInfo|_]}=DocInfo, Options) ->
    #rev_info{deleted=IsDeleted,rev={Pos,RevId},body_sp=Bp} = RevInfo,
    Doc = make_doc(Db, Id, IsDeleted, Bp, {Pos,[RevId]}),
    {ok, Doc#doc{meta=doc_meta_info(DocInfo, [], Options)}};
open_doc_int(Db, #full_doc_info{id=Id,rev_tree=RevTree}=FullDocInfo, Options) ->
    #doc_info{revs=[#rev_info{deleted=IsDeleted,rev=Rev,body_sp=Bp}|_]} =
        DocInfo = couch_doc:to_doc_info(FullDocInfo),
    {[{_, RevPath}], []} = couch_key_tree:get(RevTree, [Rev]),
    Doc = make_doc(Db, Id, IsDeleted, Bp, RevPath),
    {ok, Doc#doc{meta=doc_meta_info(DocInfo, RevTree, Options)}};
open_doc_int(Db, Id, Options) ->
    case get_full_doc_info(Db, Id) of
    {ok, FullDocInfo} ->
        open_doc_int(Db, FullDocInfo, Options);
    not_found ->
        {not_found, missing}
    end.

doc_meta_info(#doc_info{high_seq=Seq,revs=[#rev_info{rev=Rev}|RestInfo]}, RevTree, Options) ->
    case lists:member(revs_info, Options) of
    false -> [];
    true ->
        {[{Pos, RevPath}],[]} =
            couch_key_tree:get_full_key_paths(RevTree, [Rev]),

        [{revs_info, Pos, lists:map(
            fun({Rev1, {true, _Sp, _UpdateSeq}}) ->
                {Rev1, deleted};
            ({Rev1, {false, _Sp, _UpdateSeq}}) ->
                {Rev1, available};
            ({Rev1, ?REV_MISSING}) ->
                {Rev1, missing}
            end, RevPath)}]
    end ++
    case lists:member(conflicts, Options) of
    false -> [];
    true ->
        case [Rev1 || #rev_info{rev=Rev1,deleted=false} <- RestInfo] of
        [] -> [];
        ConflictRevs -> [{conflicts, ConflictRevs}]
        end
    end ++
    case lists:member(deleted_conflicts, Options) of
    false -> [];
    true ->
        case [Rev1 || #rev_info{rev=Rev1,deleted=true} <- RestInfo] of
        [] -> [];
        DelConflictRevs -> [{deleted_conflicts, DelConflictRevs}]
        end
    end ++
    case lists:member(local_seq, Options) of
    false -> [];
    true -> [{local_seq, Seq}]
    end.

read_doc(#db{fd=Fd}, Pos) ->
    couch_file:pread_term(Fd, Pos).


doc_to_tree(#doc{revs={Start, RevIds}}=Doc) ->
    [Tree] = doc_to_tree_simple(Doc, lists:reverse(RevIds)),
    {Start - length(RevIds) + 1, Tree}.


doc_to_tree_simple(Doc, [RevId]) ->
    [{RevId, Doc, []}];
doc_to_tree_simple(Doc, [RevId | Rest]) ->
    [{RevId, ?REV_MISSING, doc_to_tree_simple(Doc, Rest)}].


make_doc(#db{fd=Fd}=Db, Id, Deleted, Bp, RevisionPath) ->
    {BodyData, Atts} =
    case Bp of
    nil ->
        {[], []};
    _ ->
        {ok, {BodyData0, Atts0}} = read_doc(Db, Bp),
        {BodyData0,
            lists:map(
                fun({Name,Type,Sp,AttLen,DiskLen,RevPos,Md5,Enc}) ->
                    #att{name=Name,
                        type=Type,
                        att_len=AttLen,
                        disk_len=DiskLen,
                        md5=Md5,
                        revpos=RevPos,
                        data={Fd,Sp},
                        encoding=
                            case Enc of
                            true ->
                                % 0110 UPGRADE CODE
                                gzip;
                            false ->
                                % 0110 UPGRADE CODE
                                identity;
                            _ ->
                                Enc
                            end
                    };
                ({Name,Type,Sp,AttLen,RevPos,Md5}) ->
                    #att{name=Name,
                        type=Type,
                        att_len=AttLen,
                        disk_len=AttLen,
                        md5=Md5,
                        revpos=RevPos,
                        data={Fd,Sp}};
                ({Name,{Type,Sp,AttLen}}) ->
                    #att{name=Name,
                        type=Type,
                        att_len=AttLen,
                        disk_len=AttLen,
                        md5= <<>>,
                        revpos=0,
                        data={Fd,Sp}}
                end, Atts0)}
    end,
    #doc{
        id = Id,
        revs = RevisionPath,
        body = BodyData,
        atts = Atts,
        deleted = Deleted
        }.

