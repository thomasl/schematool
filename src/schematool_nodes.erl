%%% File    : schematool_nodes.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 29 Jan 2014 by Thomas Lindgren <>

%% Node management/migration for schema
%% purposes.
%%
%% In essence, this concerns figuring out node
%% changes and adding and deleting nodes "in a
%% good order".

-module(schematool_nodes).
-export(
   [
    diff/2,
    alter_nodes/2,
    alter_nodes/4,
    node_available/1,
    add_schema_node/1
   ]).

-define(dbg(Str, Xs), io:format(Str, Xs)).
%-define(dbg(Str,Xs), ok).

%% Preparation (proposed)
%% - collect all node names that need to be available
%% - ping them prior to starting

%% Use this prior to adding table copies, etc.

node_available(N) ->
    case net_adm:ping(N) of
	pong ->
	    case rpc:call(N, mnesia, start, []) of
		ok ->
		    true;
		Err ->
		    ?dbg("Unable to start mnesia on ~p: ~p\n",
			 [N, Err]),
		    false
	    end;
	pang ->
	    ?dbg("Node unreachable: ~p\n", [N]),
	    false
    end.

%% Add node N to hold the schematool tables.
%%
%% NB: add_table_copy/3 seems to run in its own txn,
%%  returns 

add_schema_node(N) ->
    true = node_available(N),
    TabType = schematool:schematool_table_type(),
    lists:foreach(
      fun(Tab) ->
	      case mnesia:add_table_copy(
		     schematool_info, 
		     N,
		     TabType) of
		  {atomic, ok} ->
		      ok;
		  {aborted, {already_exists, Tab, N}} ->
		      ok;
		  {aborted, {not_active, Tab, N}} ->
		      %% ruh-roh
		      exit(nyi);
		  {aborted, Other} ->
		      io:format("add_schema_node(~p) ABORT -> ~w\n", [N, Other]),
		      exit(nyi)
	      end
      end,
      schematool:schematool_tables()
     ).

% {aborted,{already_exists,schematool_info,_}}
% {aborted,{not_active,schema,N}}

%% Delete schema node = delete schematool table copies
%%
%% Returns bool, aborts if some table copy can't be deleted.
%%
%% Run in txn (due to del_table_copy)

delete_schema_node(N) ->
    true = node_available(N),
    lists:foreach(
      fun(Tab) ->
	      case mnesia:del_table_copy(Tab, N) of
		  {atomic, ok} ->
		      ok;
		  {aborted, Err} ->
		      io:format("delete_schema_node(~p) ABORT -> ~w\n",
				[N, Err]),
		      exit(nyi)
	      end
      end,
      schematool:schematool_tables()).
    
%% Input: lists of nodes
%% Output: {Added, Remaining, Deleted},
%%  which are lists of nodes
%%
%% Remaining = Cut = Old /\ New
%% Added = New \ Cut
%% Deleted = Old \ Cut

diff(Old_lst, New_lst) ->
    Old = sets:from_list(Old_lst),
    New = sets:from_list(New_lst),
    Cut = sets:intersection(Old, New),
    Deleted = sets:subtract(Old, Cut),
    Added = sets:subtract(New, Cut),
    {to_list(Added), 
     to_list(Cut), 
     to_list(Deleted)
    }.

to_list(Set) ->
    lists:sort(sets:to_list(Set)).

%%
%% -1. malformed: add,del,rem all empty
%% 0. unchanged. [common enough]
%% 1. expanding: deleted empty
%% 2. contracting: added empty
%% 3. rebalance: remaining non-empty
%% 4. migrate: remaining empty
%%
%% These return instructions for what to do
%% (in the given order).

%% (convenience)

alter_nodes(Schema, {Add, Rem, Del}) ->
    alter_nodes(Schema, Add, Rem, Del).

%% The following implements the instructions to be performed
%% when doing the migrations. We have a number of special cases,
%% with the most complex one being when the old and new nodes
%% do not overlap (called the nocommon case).
%%
%% Migration of nodes for the nocommon case:
%% Option 1: 
%% - create new nodes with old schema
%% - add new nodes as table copies
%% - migrate all the tables
%% - delete the old nodes
%%
%% Option 2:
%% - create new nodes with new schema
%% - migrate old nodes to new schema
%% - add new nodes as table copies
%% - delete old nodes
%% 
%% UNFINISHED
%% - do we need to pass the old schema too for migrations?

alter_nodes(_NewSchema, [], [], []) ->
    exit(no_nodes);
alter_nodes(NewSchema, [], Remaining, []) when Remaining =/= [] ->
    %% unchanged, do nothing
    [migrate_nodes(NewSchema, Remaining)];
alter_nodes(Schema, Added, Remaining, []) when Remaining =/= []  ->
    %% add new nodes, then replicate
    [create_nodes(Schema, Added),
     migrate_nodes(Schema, Remaining)];
alter_nodes(Schema, [], Remaining, Deleted) when Remaining =/= []  ->
    %% old nodes deleted
    [delete_nodes(Deleted),
     migrate_nodes(Schema, Remaining)];
alter_nodes(Schema, Added, Remaining, Deleted)  when Remaining =/= [] ->
    %% switch out some nodes and switch in others
    %% - since there is a core of remaining nodes, we can
    %%   get rid of the deleted nodes first
    %% 
    %% migrating: first delete dead copies, then add new copies
    %% and replicate from the remaining
    [delete_nodes(Deleted),
     create_nodes(Schema, Added),
     migrate_nodes(Schema, Remaining)];
alter_nodes(Schema, Added, [], Deleted) ->
    %% migrate between non-overlapping sets of nodes; we
    %% need to replicate before abandoning the old nodes
    %%
    %% add new nodes, replicate to them
    %% then delete the original nodes
    %% 
    %% NB: might be enough to add one node, replicate to that
    %%  then delete old nodes, then add the rest
    %%  - not sure about trade off?
    %%
    [create_nodes(Schema, Added),
     migrate_nodes(Schema, Added),
     delete_nodes(Deleted)];
alter_nodes(Schema, Added, Remaining, Deleted) ->
    exit({unhandled_case, {alter_nodes, Schema, Added, Remaining, Deleted}}).

%% Create schema on nodes (Schema is the generated schema module).

create_nodes(Schema, Ns) ->
    [ {schematool_helper, create_schema, [Schema, N]} || N <- Ns ].

%% Delete schema on nodes Ns
%%
%% UNFINISHED
%% - trigger system backup before this is done? (only on the nodes?)

delete_nodes(Ns) ->
    [ {schematool_helper, delete_schema, [N]} || N <- Ns ].

%% UNFINISHED
%% - just a placeholder at the moment, what needs to be done?

migrate_nodes(NewSchema, Ns) ->
    [{error, {{schematool_helper, migrate_nodes, [NewSchema, Ns]}, nyi}}].
