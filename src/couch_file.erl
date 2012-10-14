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

-module(couch_file).

-include("couch_db.hrl").

-define(SIZE_BLOCK, 4096).

-record(file, {
    fd,
    tail_append_begin = 0, % 09 UPGRADE CODE
    eof = 0,
    path = ""
    }).

-export([open/1, open/2, close/1, bytes/1, sync/1, append_binary/2]).
-export([append_term/2, pread_term/2, pread_iolist/2]).
-export([pread_binary/2, read_header/1, truncate/2]).
-export([append_term_md5/2,append_binary_md5/2]).
-export([delete/2,delete/3,init_delete_dir/1]).

-export([open_ns/2, pread_iolist_ns/2, pread_ns/3, bytes_ns/1, sync_ns/1, find_header_ns/1]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    case gen_server:start_link(couch_file,
            {Filepath, Options, self(), Ref = make_ref()}, []) of
    {ok, Fd} ->
        {ok, Fd};
    ignore ->
        % get the error
        receive
        {Ref, Pid, Error} ->
            case process_info(self(), trap_exit) of
            {trap_exit, true} -> receive {'EXIT', Pid, _} -> ok end;
            {trap_exit, false} -> ok
            end,
            Error
        end;
    Error ->
        Error
    end.


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang term to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_term(Fd, Term) ->
    append_binary(Fd, term_to_binary(Term)).
    
append_term_md5(Fd, Term) ->
    append_binary_md5(Fd, term_to_binary(Term)).


%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos} where Pos is the file offset to the beginning the
%%  serialized  term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------

append_binary(Fd, Bin) ->
    Size = iolist_size(Bin),
    gen_server:call(Fd, {append_bin,
            [<<0:1/integer,Size:31/integer>>, Bin]}, infinity).
    
append_binary_md5(Fd, Bin) ->
    Size = iolist_size(Bin),
    gen_server:call(Fd, {append_bin,
            [<<1:1/integer,Size:31/integer>>, couch_util:md5(Bin), Bin]}, infinity).


%%----------------------------------------------------------------------
%% Purpose: Reads a term from a file that was written with append_term
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------


pread_term(Fd, Pos) ->
    {ok, Bin} = pread_binary(Fd, Pos),
    {ok, binary_to_term(Bin)}.


%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

pread_binary(Fd, Pos) ->
    {ok, L} = pread_iolist(Fd, Pos),
    {ok, iolist_to_binary(L)}.


pread_iolist(Fd, Pos) ->
    case gen_server:call(Fd, {pread_iolist, Pos}, infinity) of
    {ok, IoList, <<>>} ->
        {ok, IoList};
    {ok, IoList, Md5} ->
        case couch_util:md5(IoList) of
        Md5 ->
            {ok, IoList};
        _ ->
            exit({file_corruption, <<"file corruption">>})
        end;
    Error ->
        Error
    end.

%%----------------------------------------------------------------------
%% Purpose: The length of a file, in bytes.
%% Returns: {ok, Bytes}
%%  or {error, Reason}.
%%----------------------------------------------------------------------

% length in bytes
bytes(Fd) ->
    gen_server:call(Fd, bytes, infinity).

%%----------------------------------------------------------------------
%% Purpose: Truncate a file to the number of bytes.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

truncate(Fd, Pos) ->
    gen_server:call(Fd, {truncate, Pos}, infinity).

%%----------------------------------------------------------------------
%% Purpose: Ensure all bytes written to the file are flushed to disk.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------

sync(Filepath) when is_list(Filepath) ->
    {ok, Fd} = file:open(Filepath, [append, raw]),
    try file:sync(Fd) after file:close(Fd) end;
sync(Fd) ->
    gen_server:call(Fd, sync, infinity).

%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    MRef = erlang:monitor(process, Fd),
    try
        catch unlink(Fd),
        catch exit(Fd, shutdown),
        receive
        {'DOWN', MRef, _, _, _} ->
            ok
        end
    after
        erlang:demonitor(MRef, [flush])
    end.


delete(RootDir, Filepath) ->
    delete(RootDir, Filepath, true).


delete(RootDir, Filepath, Async) ->
    DelFile = filename:join([RootDir,".delete", ?b2l(couch_uuids:random())]),
    case file:rename(Filepath, DelFile) of
    ok ->
        if (Async) ->
            spawn(file, delete, [DelFile]),
            ok;
        true ->
            file:delete(DelFile)
        end;
    Error ->
        Error
    end.


init_delete_dir(RootDir) ->
    Dir = filename:join(RootDir,".delete"),
    % note: ensure_dir requires an actual filename companent, which is the
    % reason for "foo".
    filelib:ensure_dir(filename:join(Dir,"foo")),
    filelib:fold_files(Dir, ".*", true,
        fun(Filename, _) ->
            ok = file:delete(Filename)
        end, ok).

read_header(Fd) ->
    case gen_server:call(Fd, find_header, infinity) of
    {ok, Bin} ->
        {ok, binary_to_term(Bin)};
    Else ->
        Else
    end.

-define(HEADER_SIZE, 2048). % size of each segment of the doubly written header


find_header(_Fd, -1) ->
    no_valid_header;
find_header(Fd, Block) ->
    case (catch load_header(Fd, Block)) of
    {ok, Bin} ->
        {ok, Bin};
    _Error ->
        find_header(Fd, Block -1)
    end.

load_header(Fd, Block) ->
    {ok, <<1>>} = file:pread(Fd, Block*?SIZE_BLOCK, 1),
    {ok, <<HeaderLen:32/integer>>} = file:pread(Fd, (Block*?SIZE_BLOCK) + 1, 4),
    TotalBytes = calculate_total_read_len(1, HeaderLen),
    {ok, <<RawBin:TotalBytes/binary>>} =
            file:pread(Fd, (Block*?SIZE_BLOCK) + 5, TotalBytes),
    <<Md5Sig:16/binary, HeaderBin/binary>> =
        iolist_to_binary(remove_block_prefixes(1, RawBin)),
    Md5Sig = couch_util:md5(HeaderBin),
    {ok, HeaderBin}.

-spec read_raw_iolist_int(#file{}, Pos::non_neg_integer(), Len::non_neg_integer()) ->
    {Data::iolist(), CurPos::non_neg_integer()}.
read_raw_iolist_int(#file{fd=Fd, tail_append_begin=TAB, path=Path}, Pos, Len) ->
    BlockOffset = Pos rem ?SIZE_BLOCK,
    TotalBytes = calculate_total_read_len(BlockOffset, Len),
%%    {ok, <<RawBin:TotalBytes/binary>>} = file:pread(Fd, Pos, TotalBytes),
    %% This isn't probably the most erlang way to write this; rethink...
    case file:pread(Fd, Pos, TotalBytes) of
	{ok, <<RawBin:TotalBytes/binary>>} -> 
	      if Pos >= TAB ->
		      {remove_block_prefixes(BlockOffset, RawBin), Pos + TotalBytes};
		 true ->
		      % 09 UPGRADE CODE
		      <<ReturnBin:Len/binary, _/binary>> = RawBin,
		      {[ReturnBin], Pos + Len}
	      end;
	{ok, BinaryData} ->   
	    ?LOG_ERROR("Bad Size in read of file ~p @ ~p, expected ~p got ~p bytes", [Path, Pos, TotalBytes, size(BinaryData)]),
	    couch_stats_collector:increment({couchdb, crash_count_trapped}),
	    throw({badmatch, erlang:get_stacktrace()});
	X ->
	    ?LOG_ERROR("Reading file ~p@~p, got ~p", [Path, Pos, X]),
	    couch_stats_collector:increment({couchdb, crash_count_trapped}),
	    throw({X, erlang:get_stacktrace()})
    end.

-spec extract_md5(iolist()) -> {binary(), iolist()}.
extract_md5(FullIoList) ->
    {Md5List, IoList} = split_iolist(FullIoList, 16, []),
    {iolist_to_binary(Md5List), IoList}.

calculate_total_read_len(0, FinalLen) ->
    calculate_total_read_len(1, FinalLen) + 1;
calculate_total_read_len(BlockOffset, FinalLen) ->
    case ?SIZE_BLOCK - BlockOffset of
    BlockLeft when BlockLeft >= FinalLen ->
        FinalLen;
    BlockLeft ->
        FinalLen + ((FinalLen - BlockLeft) div (?SIZE_BLOCK -1)) +
            if ((FinalLen - BlockLeft) rem (?SIZE_BLOCK -1)) =:= 0 -> 0;
                true -> 1 end
    end.

remove_block_prefixes(_BlockOffset, <<>>) ->
    [];
remove_block_prefixes(0, <<_BlockPrefix,Rest/binary>>) ->
    remove_block_prefixes(1, Rest);
remove_block_prefixes(BlockOffset, Bin) ->
    BlockBytesAvailable = ?SIZE_BLOCK - BlockOffset,
    case size(Bin) of
    Size when Size > BlockBytesAvailable ->
        <<DataBlock:BlockBytesAvailable/binary,Rest/binary>> = Bin,
        [DataBlock | remove_block_prefixes(0, Rest)];
    _Size ->
        [Bin]
    end.

%% @doc Returns a tuple where the first element contains the leading SplitAt
%% bytes of the original iolist, and the 2nd element is the tail. If SplitAt
%% is larger than byte_size(IoList), return the difference.
-spec split_iolist(IoList::iolist(), SplitAt::non_neg_integer(), Acc::list()) ->
    {iolist(), iolist()} | non_neg_integer().
split_iolist(List, 0, BeginAcc) ->
    {lists:reverse(BeginAcc), List};
split_iolist([], SplitAt, _BeginAcc) ->
    SplitAt;
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) when SplitAt > byte_size(Bin) ->
    split_iolist(Rest, SplitAt - byte_size(Bin), [Bin | BeginAcc]);
split_iolist([<<Bin/binary>> | Rest], SplitAt, BeginAcc) ->
    <<Begin:SplitAt/binary,End/binary>> = Bin,
    split_iolist([End | Rest], 0, [Begin | BeginAcc]);
split_iolist([Sublist| Rest], SplitAt, BeginAcc) when is_list(Sublist) ->
    case split_iolist(Sublist, SplitAt, BeginAcc) of
    {Begin, End} ->
        {Begin, [End | Rest]};
    SplitRemaining ->
        split_iolist(Rest, SplitAt - (SplitAt - SplitRemaining), [Sublist | BeginAcc])
    end;
split_iolist([Byte | Rest], SplitAt, BeginAcc) when is_integer(Byte) ->
    split_iolist(Rest, SplitAt - 1, [Byte | BeginAcc]).


%%
%% Factored out gen_server funs...
%% 
pread_iolist_ns(File, Pos) ->
    {LenIolist, NextPos} = read_raw_iolist_int(File, Pos, 4),
    case iolist_to_binary(LenIolist) of
        <<1:1/integer,Len:31/integer>> -> % an MD5-prefixed term
            {Md5AndIoList, _} = read_raw_iolist_int(File, NextPos, Len+16),
            {Md5, IoList} = extract_md5(Md5AndIoList),
            {ok, IoList, Md5};
    <<0:1/integer,Len:31/integer>> ->
            {Iolist, _} = read_raw_iolist_int(File, NextPos, Len),
            {ok, Iolist, <<>>}
    end.

pread_ns(#file{fd=Fd,tail_append_begin=TailAppendBegin} = _File, Pos, Bytes) ->
    {ok, Bin} = file:pread(Fd, Pos, Bytes),
    {ok, Bin, Pos >= TailAppendBegin}.

open_ns(FilePath, _Options) ->
    {ok, Fd} = file:open(FilePath, [read, append, raw, binary]),
    {ok, Length} = file:position(Fd, eof),
    {ok, #file{fd=Fd, eof=Length, path=FilePath}}.

bytes_ns(#file{eof=Length}) ->
    Length.

sync_ns(#file{fd=Fd}) ->
    file:sync(Fd).

find_header_ns(#file{fd=Fd, eof=Pos}) ->
    find_header(Fd, Pos div ?SIZE_BLOCK).
