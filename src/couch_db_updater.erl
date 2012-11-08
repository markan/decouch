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

-module(couch_db_updater).

-export([btree_by_id_reduce/2,btree_by_seq_reduce/2]).
-export([init_db/4]).

-include("couch_db.hrl").
-include_lib("kernel/include/file.hrl").

btree_by_seq_split(#doc_info{id=Id, high_seq=KeySeq, revs=Revs}) ->
    RevInfos = [{Rev, Seq, Bp} ||
        #rev_info{rev=Rev,seq=Seq,deleted=false,body_sp=Bp} <- Revs],
    DeletedRevInfos = [{Rev, Seq, Bp} ||
        #rev_info{rev=Rev,seq=Seq,deleted=true,body_sp=Bp} <- Revs],
    {KeySeq,{Id, RevInfos, DeletedRevInfos}}.

btree_by_seq_join(KeySeq, {Id, RevInfos, DeletedRevInfos}) ->
    #doc_info{
        id = Id,
        high_seq=KeySeq,
        revs =
            [#rev_info{rev=Rev,seq=Seq,deleted=false,body_sp = Bp} ||
                {Rev, Seq, Bp} <- RevInfos] ++
            [#rev_info{rev=Rev,seq=Seq,deleted=true,body_sp = Bp} ||
                {Rev, Seq, Bp} <- DeletedRevInfos]};
btree_by_seq_join(KeySeq,{Id, Rev, Bp, Conflicts, DelConflicts, Deleted}) ->
    % 09 UPGRADE CODE
    % this is the 0.9.0 and earlier by_seq record. It's missing the body pointers
    % and individual seq nums for conflicts that are currently in the index,
    % meaning the filtered _changes api will not work except for on main docs.
    % Simply compact a 0.9.0 database to upgrade the index.
    #doc_info{
        id=Id,
        high_seq=KeySeq,
        revs = [#rev_info{rev=Rev,seq=KeySeq,deleted=Deleted,body_sp=Bp}] ++
            [#rev_info{rev=Rev1,seq=KeySeq,deleted=false} || Rev1 <- Conflicts] ++
            [#rev_info{rev=Rev2,seq=KeySeq,deleted=true} || Rev2 <- DelConflicts]}.

btree_by_id_split(#full_doc_info{id=Id, update_seq=Seq,
        deleted=Deleted, rev_tree=Tree}) ->
    DiskTree =
    couch_key_tree:map(
        fun(_RevId, {IsDeleted, BodyPointer, UpdateSeq}) ->
            {if IsDeleted -> 1; true -> 0 end, BodyPointer, UpdateSeq};
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING
        end, Tree),
    {Id, {Seq, if Deleted -> 1; true -> 0 end, DiskTree}}.

btree_by_id_join(Id, {HighSeq, Deleted, DiskTree}) ->
    Tree =
    couch_key_tree:map(
        fun(_RevId, {IsDeleted, BodyPointer, UpdateSeq}) ->
            {IsDeleted == 1, BodyPointer, UpdateSeq};
        (_RevId, ?REV_MISSING) ->
            ?REV_MISSING;
        (_RevId, {IsDeleted, BodyPointer}) ->
            % 09 UPGRADE CODE
            % this is the 0.9.0 and earlier rev info record. It's missing the seq
            % nums, which means couchdb will sometimes reexamine unchanged
            % documents with the _changes API.
            % This is fixed by compacting the database.
            {IsDeleted == 1, BodyPointer, HighSeq}
        end, DiskTree),

    #full_doc_info{id=Id, update_seq=HighSeq, deleted=Deleted==1, rev_tree=Tree}.

btree_by_id_reduce(reduce, FullDocInfos) ->
    % count the number of not deleted documents
    {length([1 || #full_doc_info{deleted=false} <- FullDocInfos]),
        length([1 || #full_doc_info{deleted=true} <- FullDocInfos])};
btree_by_id_reduce(rereduce, Reds) ->
    {lists:sum([Count || {Count,_} <- Reds]),
        lists:sum([DelCount || {_, DelCount} <- Reds])}.

btree_by_seq_reduce(reduce, DocInfos) ->
    % count the number of documents
    length(DocInfos);
btree_by_seq_reduce(rereduce, Reds) ->
    lists:sum(Reds).

simple_upgrade_record(Old, New) when tuple_size(Old) =:= tuple_size(New) ->
    Old;
simple_upgrade_record(Old, New) when tuple_size(Old) < tuple_size(New) ->
    OldSz = tuple_size(Old),
    NewValuesTail =
        lists:sublist(tuple_to_list(New), OldSz + 1, tuple_size(New) - OldSz),
    list_to_tuple(tuple_to_list(Old) ++ NewValuesTail).


init_db(DbName, Filepath, Fd, Header0) ->
    Header1 = simple_upgrade_record(Header0, #db_header{}),
    Header =
    case element(2, Header1) of
    1 -> Header1#db_header{unused = 0, security_ptr = nil}; % 0.9
    2 -> Header1#db_header{unused = 0, security_ptr = nil}; % post 0.9 and pre 0.10
    3 -> Header1#db_header{security_ptr = nil}; % post 0.9 and pre 0.10
    4 -> Header1#db_header{security_ptr = nil}; % 0.10 and pre 0.11
    ?LATEST_DISK_VERSION -> Header1;
    _ -> throw({database_disk_version_error, "Incorrect disk header version"})
    end,

    {ok, FsyncOptions} = {ok, [before_header, after_header, on_file_open]},
% couch_util:parse_term(
%            couch_config:get("couchdb", "fsync_options",
%                    "[before_header, after_header, on_file_open]")),
%                           "before_header").

    {ok, IdBtree} = couch_btree:open(Header#db_header.fulldocinfo_by_id_btree_state, Fd,
        [{split, fun(X) -> btree_by_id_split(X) end},
        {join, fun(X,Y) -> btree_by_id_join(X,Y) end},
        {reduce, fun(X,Y) -> btree_by_id_reduce(X,Y) end}]),
    {ok, SeqBtree} = couch_btree:open(Header#db_header.docinfo_by_seq_btree_state, Fd,
            [{split, fun(X) -> btree_by_seq_split(X) end},
            {join, fun(X,Y) -> btree_by_seq_join(X,Y) end},
            {reduce, fun(X,Y) -> btree_by_seq_reduce(X,Y) end}]),
    {ok, LocalDocsBtree} = couch_btree:open(Header#db_header.local_docs_btree_state, Fd),
    case Header#db_header.security_ptr of
    nil ->
        Security = [],
        SecurityPtr = nil;
    SecurityPtr ->
        {ok, Security} = couch_file:pread_term(Fd, SecurityPtr)
    end,
    % convert start time tuple to microsecs and store as a binary string
    {MegaSecs, Secs, MicroSecs} = now(),
    StartTime = ?l2b(io_lib:format("~p",
            [(MegaSecs*1000000*1000000) + (Secs*1000000) + MicroSecs])),
%%    {ok, RefCntr} = couch_ref_counter:start([Fd]),
    #db{
        update_pid=self(),
        fd=Fd,
        header=Header,
        fulldocinfo_by_id_btree = IdBtree,
        docinfo_by_seq_btree = SeqBtree,
        local_docs_btree = LocalDocsBtree,
        committed_update_seq = Header#db_header.update_seq,
        update_seq = Header#db_header.update_seq,
        name = DbName,
        filepath = Filepath,
        security = Security,
        security_ptr = SecurityPtr,
        instance_start_time = StartTime,
        revs_limit = Header#db_header.revs_limit,
        fsync_options = FsyncOptions,
	compact_seq = Header#db_header.compact_seq
        }.



