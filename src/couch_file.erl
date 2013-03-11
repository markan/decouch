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

-include_lib("kernel/include/file.hrl").

-include("couch_db.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(SIZE_BLOCK, 4096).

-record(file, {
    fd,
    tail_append_begin = 0, % 09 UPGRADE CODE
    eof = 0,
    path = ""
    }).

-export([open/1, open/2, close/1, bytes/1]).
-export([pread_term/2, pread_iolist/2]).
-export([pread_binary/2, read_header/1]).
-export([close_ns/1]).
-export([open_ns/2, pread_iolist_ns/2, pread_ns/3, bytes_ns/1, sync_ns/1, find_header_ns/1]).

-export([read_all/1]).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------

open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    open_ns(Filepath, Options).


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
    case pread_iolist_ns(Fd, Pos) of
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
    bytes_ns(Fd).


%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(#db{fd=File}) ->
    close_ns(File).

read_header(Fd) ->
    {ok, Term} = find_header_ns(Fd),
    {ok, binary_to_term(Term)}.

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
            throw({badmatch, erlang:get_stacktrace()});
        X ->
            ?LOG_ERROR("Reading file ~p@~p, got ~p", [Path, Pos, X]),
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
    {ok, FileInfo} = file:read_file_info(FilePath),
    Size = FileInfo#file_info.size,
    MaxReadAhead = envy:get(decouch, max_read_ahead, 1024*1024*1024, integer),
    ReadAhead = min(Size, MaxReadAhead),
    %% We know we are going to read the whole file, so we should bite
    %% the bullet and read ahead the whole thing. Couchdb's access
    %% pattern is from end towards beginning so it actively subverts
    %% readahead
    {ok, Fd} = file:open(FilePath, [read, append, raw, binary, {read_ahead, ReadAhead}]),
    %% Hopefully trigger readahead starting from the beginning of the file
    {Time, _} = timer:tc(couch_file, read_all, [Fd]),
    ?LOG_DEBUG("Database '~s' of size ~w bytes prefetched in ~f seconds~n", [FilePath, Size, Time/1000000]),
    {ok, Length} = file:position(Fd, eof),
    {ok, #file{fd=Fd, eof=Length, path=FilePath}}.

%%%
%%% This a hack to read the whole file in before we actually try to read as a couch file
%%% 
read_all(Fd) ->
    case file:read(Fd, 1048576) of %% 2^20 bytes == 1048576
        {ok, _Data} ->
            read_all(Fd);
        {error, enomem} ->
            erlang:garbage_collect(),
            ok;
        eof->
            ok
    end.

bytes_ns(#file{eof=Length}) ->
    Length.

sync_ns(#file{fd=Fd}) ->
    file:sync(Fd).

find_header_ns(#file{fd=Fd, eof=Pos}) ->
    find_header(Fd, Pos div ?SIZE_BLOCK).

close_ns(#file{fd=Fd}) ->
    file:close(Fd).
