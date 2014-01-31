%%% File    : schematool_helper.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 31 Jan 2014 by Thomas Lindgren <>

%% Helper functions for operating on schemas, etc.

-module(schematool_helper).
-export(
   [create_schema/2,
    delete_schema/1
   ]).

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

