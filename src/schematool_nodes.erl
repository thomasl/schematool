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
    alter_nodes/1,
    alter_nodes/3
   ]).

-define(dbg(Str, Xs), io:format(Str, Xs)).
%-define(dbg(Str,Xs), ok).

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

%% When altering the collection of nodes,
%% we have a general scenario and a number
%% of simplifications.
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

alter_nodes({Add, Rem, Del}) ->
    alter_nodes(Add, Rem, Del).

%%

alter_nodes([], [], []) ->
    exit(no_nodes);
alter_nodes([], Remaining, []) when Remaining =/= [] ->
    %% unchanged, do nothing
    [];
alter_nodes(Added, Remaining, []) when Remaining =/= []  ->
    %% add new nodes, then replicate
    [{expand, Added},
     replicate];
alter_nodes([], Remaining, Deleted) when Remaining =/= []  ->
    %% old nodes deleted
    [{contract, Deleted}];
alter_nodes(Added, Remaining, Deleted)  when Remaining =/= [] ->
    %% switch out some nodes and switch in others
    %% - since there is a core of remaining nodes, we can
    %%   get rid of the deleted nodes first
    %% 
    %% migrating: first delete dead copies, then add new copies
    %% and replicate from the remaining
    [{contract, Deleted},
     {expand, Added},
     replicate];
alter_nodes(Added, [], Deleted) ->
    %% migrate between non-overlapping sets of nodes; we
    %% need to replicate before abandoning the old nodes
    %%
    %% add new nodes, replicate to them
    %% then delete the original nodes
    %% 
    %% NB: might be enough to add one node, replicate to that
    %%  then delete old nodes, then add the rest
    %%  - not sure about trade off?
    [{expand, Added},
     replicate,
     {contract, Deleted}].

execute_instructions(Instrs) ->
    lists:foreach(
      fun({expand, Nodes}) ->
	      io:format("NYI: add nodes ~p\n", [Nodes]);
	 ({contract, Nodes}) ->
	      io:format("NYI: delete nodes ~p\n", [Nodes]);
	 (replicate) ->
	      io:format("NYI: replicate tables everywhere\n", []);
	 (Other) ->
	      ok
      end,
      Instrs).

    
