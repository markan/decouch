-module(decouch_reader).

-include_lib("eunit/include/eunit.hrl").

-include("couch_db.hrl").

-export([open/0]).
-export([open/1]).

open() ->
    open("authorization.couch").

open(FilePath) ->
    DbName = "foo",
    {ok, Fd} = couch_db:open_db_file(FilePath, []),
    {ok, Header} = couch_file:read_header(Fd),
    ?debugVal(Header),
    Db = couch_db_updater:init_db(DbName, FilePath, Fd, Header),
    ?debugVal(Db),
    {Db, Header}.



