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

-module(couch_stream).

-define(FILE_POINTER_BYTES, 8).
-define(FILE_POINTER_BITS, 8*(?FILE_POINTER_BYTES)).

-define(STREAM_OFFSET_BYTES, 4).
-define(STREAM_OFFSET_BITS, 8*(?STREAM_OFFSET_BYTES)).

-define(HUGE_CHUNK, 1000000000). % Huge chuck size when reading all in one go

-define(DEFAULT_STREAM_CHUNK, 16#00100000). % 1 meg chunks when streaming data

-export([foldl/4, foldl/5, foldl_decode/6, old_foldl/5]).
-export([old_read_term/2]).

-include("couch_db.hrl").

%%% Interface functions %%%

% 09 UPGRADE CODE
old_foldl(_Fd, null, 0, _Fun, Acc) ->
    Acc;
old_foldl(Fd, OldPointer, Len, Fun, Acc) when is_tuple(OldPointer)->
    {ok, Acc2, _} = old_stream_data(Fd, OldPointer, Len, ?DEFAULT_STREAM_CHUNK, Fun, Acc),
    Acc2.

foldl(_Fd, [], _Fun, Acc) ->
    Acc;
foldl(Fd, [Pos|Rest], Fun, Acc) ->
    {ok, Bin} = couch_file:pread_iolist(Fd, Pos),
    foldl(Fd, Rest, Fun, Fun(Bin, Acc)).

foldl(Fd, PosList, <<>>, Fun, Acc) ->
    foldl(Fd, PosList, Fun, Acc);
foldl(Fd, PosList, Md5, Fun, Acc) ->
    foldl(Fd, PosList, Md5, couch_util:md5_init(), Fun, Acc).

foldl_decode(Fd, PosList, Md5, Enc, Fun, Acc) ->
    {DecDataFun, DecEndFun} = case Enc of
    gzip ->
        ungzip_init();
    identity ->
        identity_enc_dec_funs()
    end,
    Result = foldl_decode(
        DecDataFun, Fd, PosList, Md5, couch_util:md5_init(), Fun, Acc
    ),
    DecEndFun(),
    Result.

foldl(_Fd, [], Md5, Md5Acc, _Fun, Acc) ->
    Md5 = couch_util:md5_final(Md5Acc),
    Acc;
foldl(Fd, [Pos], Md5, Md5Acc, Fun, Acc) ->
    {ok, Bin} = couch_file:pread_iolist(Fd, Pos),
    Md5 = couch_util:md5_final(couch_util:md5_update(Md5Acc, Bin)),
    Fun(Bin, Acc);
foldl(Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc) ->
    {ok, Bin} = couch_file:pread_iolist(Fd, Pos),
    foldl(Fd, Rest, Md5, couch_util:md5_update(Md5Acc, Bin), Fun, Fun(Bin, Acc)).

foldl_decode(_DecFun, _Fd, [], Md5, Md5Acc, _Fun, Acc) ->
    Md5 = couch_util:md5_final(Md5Acc),
    Acc;
foldl_decode(DecFun, Fd, [Pos], Md5, Md5Acc, Fun, Acc) ->
    {ok, EncBin} = couch_file:pread_iolist(Fd, Pos),
    Md5 = couch_util:md5_final(couch_util:md5_update(Md5Acc, EncBin)),
    Bin = DecFun(EncBin),
    Fun(Bin, Acc);
foldl_decode(DecFun, Fd, [Pos|Rest], Md5, Md5Acc, Fun, Acc) ->
    {ok, EncBin} = couch_file:pread_iolist(Fd, Pos),
    Bin = DecFun(EncBin),
    Md5Acc2 = couch_util:md5_update(Md5Acc, EncBin),
    foldl_decode(DecFun, Fd, Rest, Md5, Md5Acc2, Fun, Fun(Bin, Acc)).

ungzip_init() ->
    Z = zlib:open(),
    zlib:inflateInit(Z, 16 + 15),
    {
        fun(Data) ->
            zlib:inflate(Z, Data)
        end,
        fun() ->
            ok = zlib:inflateEnd(Z),
            ok = zlib:close(Z)
        end
    }.

identity_enc_dec_funs() ->
    {
        fun(Data) -> Data end,
        fun() -> [] end
    }.

% 09 UPGRADE CODE
old_read_term(Fd, Sp) ->
    {ok, <<TermLen:(?STREAM_OFFSET_BITS)>>, Sp2}
        = old_read(Fd, Sp, ?STREAM_OFFSET_BYTES),
    {ok, Bin, _Sp3} = old_read(Fd, Sp2, TermLen),
    {ok, binary_to_term(Bin)}.

old_read(Fd, Sp, Num) ->
    {ok, RevBin, Sp2} = old_stream_data(Fd, Sp, Num, ?HUGE_CHUNK, fun(Bin, Acc) -> [Bin | Acc] end, []),
    Bin = list_to_binary(lists:reverse(RevBin)),
    {ok, Bin, Sp2}.

% 09 UPGRADE CODE
old_stream_data(_Fd, Sp, 0, _MaxChunk, _Fun, Acc) ->
    {ok, Acc, Sp};
old_stream_data(Fd, {Pos, 0}, Num, MaxChunk, Fun, Acc) ->
    {ok, <<NextPos:(?FILE_POINTER_BITS), NextOffset:(?STREAM_OFFSET_BITS)>>}
        = couch_file:old_pread(Fd, Pos, ?FILE_POINTER_BYTES + ?STREAM_OFFSET_BYTES),
    Sp = {NextPos, NextOffset},
    % Check NextPos is past current Pos (this is always true in a stream)
    % Guards against potential infinite loops caused by corruption.
    case NextPos > Pos of
        true -> ok;
        false -> throw({error, stream_corruption})
    end,
    old_stream_data(Fd, Sp, Num, MaxChunk, Fun, Acc);
old_stream_data(Fd, {Pos, Offset}, Num, MaxChunk, Fun, Acc) ->
    ReadAmount = lists:min([MaxChunk, Num, Offset]),
    {ok, Bin} = couch_file:old_pread(Fd, Pos, ReadAmount),
    Sp = {Pos + ReadAmount, Offset - ReadAmount},
    old_stream_data(Fd, Sp, Num - ReadAmount, MaxChunk, Fun, Fun(Bin, Acc)).


% Tests moved to tests/etap/050-stream.t

