%%% -*- erlang-indent-level: 4;indent-tabs-mode: nil; fill-column: 92 -*-
%%% ex: ts=4 sw=4 et
%%%
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License"); you may not
%%% use this file except in compliance with the License. You may obtain a copy of
%%% the License at
%%%
%%%   http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
%%% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
%%% License for the specific language governing permissions and limitations under
%%% the License.
%%%-------------------------------------------------------------------
%%% @author Mark Anderson <mark@opscode.com>
%%% @copyright (C) 2012, Opscode Inc.
%%% @doc
%%%
%%% @end
%%% Created :  8 Nov 2012 by Mark Anderson <>
%%%-------------------------------------------------------------------
-module(decouch_processor).

%% API
-export([process_couch_file/1]).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% API
%%%===================================================================

-record(org_info,
        { org_name,
          org_id,
          db_name,
          chef_ets,
          auth_ets}).

process_couch_file(OrgId) ->

    CData = ets:new(chef_data, [set,public]),
    AData = ets:new(auth_data, [set,public]),

    DbName = lists:flatten(["chef_", OrgId, ".couch"]),

    Org = #org_info{ org_name = "TBD",
                     org_id = OrgId,
                     db_name = DbName,
                     chef_ets = CData,
                     auth_ets = AData},
    IterFn = fun(Key, Body, AccIn) ->
                     process_item(Org, Key, Body),
                     AccIn
             end,
    decouch_reader:open_process_all(DbName, IterFn),
    Org.

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================

process_item(Org, Key, Body) ->
    JClass = ej:get({<<"json_class">>}, Body),
    CRType = ej:get({<<"couchrest-type">>}, Body),
    Type  = case {JClass, CRType} of 
                {undefined, undefined} -> undefined;
                {undefined, T} -> T;
                {T, undefined} -> T;
                _ ->
                    ?debugVal(Key),
                    ?debugVal(Body),
                    ?debugVal({JClass, CRType}),
                    error
            end,
    process_item_by_type(normalize_type_name(Type), Org, Key, Body),
    ok.

%% Process special types
process_item_by_type({_, node}, _Org, _Key, _Body) ->
    %% Node docs are to be ignored
    ok;
%% Process Chef:: types
process_item_by_type({chef, ChefType}, Org, Key, Body) ->
    ets:insert(Org#org_info.chef_ets, {Key, ChefType, Body});
%% Process Mixlib Authorization types
process_item_by_type({auth, AuthType}, Org, Key, Body) ->
    ets:insert(Org#org_info.chef_ets, {Key, AuthType, Body});

%% Process various unmatched types
process_item_by_type(undefined, _Org, <<"_design/", DesignDoc/binary>>, _Body) ->
    io:format("Design doc ~s~n", [DesignDoc]);
process_item_by_type(undefined, _Org, Key, Body) ->
    ?debugVal(undefined),
    ?debugVal(Key),
    ?debugVal(Body);
process_item_by_type(Type, Org, Key, Body) ->
    ?debugVal(Type),
    ets:insert(Org#org_info.chef_ets, {Key, Body}).

normalize_type_name(<<"Mixlib::Authorization::Models::Client">>) -> {auth, client};
normalize_type_name(<<"Mixlib::Authorization::Models::Container">>) -> {auth, container};
normalize_type_name(<<"Mixlib::Authorization::Models::Cookbook">>) -> {auth, cookbook};
normalize_type_name(<<"Mixlib::Authorization::Models::DataBag">>) -> {auth, databag};
normalize_type_name(<<"Mixlib::Authorization::Models::Environment">>) -> {auth, environment};
normalize_type_name(<<"Mixlib::Authorization::Models::Group">>) -> {auth, group};
normalize_type_name(<<"Mixlib::Authorization::Models::Node">>) -> {auth, node};
normalize_type_name(<<"Mixlib::Authorization::Models::Role">>) -> {auth, role};
normalize_type_name(<<"Mixlib::Authorization::Models::Sandbox">>) -> {auth, sandbox};
normalize_type_name(<<"Chef::Checksum">>) -> {chef, checksum};
normalize_type_name(<<"Chef::CookbookVersion">>) -> {chef, cookbook_version};
normalize_type_name(<<"Chef::DataBag">>) -> {chef, databag};
normalize_type_name(<<"Chef::DataBagItem">>) -> {chef, databag_item};
normalize_type_name(<<"Chef::Environment">>) -> {chef, environment};
normalize_type_name(<<"Chef::Node">>) -> {chef, node};
normalize_type_name(<<"Chef::Role">>) -> {chef, role};
normalize_type_name(<<"Chef::Sandbox">>) -> {chef, sandbox};
normalize_type_name(undefined) -> undefined.




