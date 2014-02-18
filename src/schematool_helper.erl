%%% File    : schematool_helper.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 31 Jan 2014 by Thomas Lindgren <>

%% Helper functions for operating on schemas, etc.

-module(schematool_helper).
-export(
   [create_schema/2,
    delete_schema/1,
    add_node/1,
    delete_node/1,
    create_table_all_nodes/3,
    delete_table_all_nodes/2,
    transform_table_layout/3,
    transform_table_layout/4,
    copy_table/2,
    lossy_copy_table/3
   ]).

%% Add new mnesia node N + replicate schema to it
%%
%% Subsequent actions will add explicitly defined
%% tables to this node.
%%
%% UNFINISHED
%% - need to replicate hidden schematool_* tables
%%   = mnesia:add_table_copy(...)
%% - see also schematool_nodes, this might not be used 
%%   at the moment?
%% - refactor: the tables used for schema are currently almost
%%   internal to schematool.erl, move this code there?
%%   * or to a third module

add_node(N) ->
    mnesia:create_schema(N),
    mnesia:add_table_copy(schema_info, N, disc_copies),
    mnesia:add_table_copy(schema_changelog, N, disc_copies).

%% Delete mnesia node N

delete_node(N) ->
    mnesia:delete_schema(N).

%% Create table on all nodes 
%%
%% - we should just run create_table, the Opts
%%   will/must define the right storage types
%%
%% Note that ADDING a table copy to existing table must use
%%   mnesia:add_table_copy(Tab, Node, StorageType)
%% Changing storage types should use this appropriately.
%%
%% Runs outside txn

create_table_all_nodes(_Nodes, Tab, Opts) ->
    mnesia:create_table(Tab, schematool_options:without(Opts)).

%% Delete table from all given Nodes
%%
%% - run inside txn, I think (or maybe not?)

delete_table_all_nodes(Nodes, Tab) ->
    lists:foreach(
      fun(Node) ->
	      mnesia:del_table_copy(Tab, Node)
      end,
      Nodes).

%% Create schema on Node.
%% - Note that mnesia is a bit finicky, requires
%%   that mnesia is not started yet on other node.

create_schema(SchemaMod, Node) when is_atom(SchemaMod) ->
    case rpc:call(Node, schematool, create_schema, [SchemaMod]) of
	{badrpc, _}=Err ->
	    {error, Err};
	Res ->
	    Res
    end.

%% From manual:
%% - mnesia must be stopped on all the Nodes
%% - all the Nodes must be alive
%% - deletes persistent data from nodes
%% - THINK TWICE BEFORE DOING IT
%%
%% DANGEROUS!

delete_schema(Nodes) ->
    MFA = {application, stop, [mnesia]},
    case rpc:multicall(Nodes, application, stop, [mnesia]) of
	{Results, []} ->
	    NsRes = lists:zip(Nodes, Results),
	    case results_ok(MFA, NsRes) of
		true ->
		    mnesia:delete_schema(Nodes);
		false ->
		    {error, NsRes}
	    end;
	{Results, BadNodes} when BadNodes =/= [] ->
	    %% is there any way to see which node generated
	    %% which result? do we need to pick through the BadNodes
	    %% and Nodes to do this? if so, ick
	    {error, {rpc_fail, MFA, Results, BadNodes}}
    end.

delete_simple_schema(Nodes) ->
    mnesia:delete_schema(Nodes).

results_ok(MFA, Results) ->
    lists:all(
      fun({_Node, ok}) -> 
	      true;
	 ({_Node, {error, {not_started, mnesia}}}) ->
	      true;
	 ({Node, Err}) ->
	      %% io:format("RPC error node ~p, ~p: ~p\n", [Node, MFA, Err])
	      false;
	 (Err) ->
	      %% io:format("RPC error unknown node, ~p: ~p\n", [MFA, Err])
	      false
      end,
      Results).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - just generate direct calls to schematool_transform?
%% - maybe we should put schematool_transform:table in new runtime lib?
%%   (e.g., compute schema diff at one node, run it on others)

transform_table_layout(Tab, Old_rec_def, New_rec_def) ->
    schematool_transform:table(Tab, Old_rec_def, New_rec_def).

transform_table_layout(Tab, Xforms, Old_rec_def, New_rec_def) ->
    schematool_transform:table(Tab, Xforms, Old_rec_def, New_rec_def).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

copy_table(Tab0, Tab1) ->
    schematool_table:copy_table(Tab0, Tab1).

lossy_copy_table(Coll, Tab0, Tab1) ->
    schematool_table:lossy_copy_table(Coll, Tab0, Tab1).

