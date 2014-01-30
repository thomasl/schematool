%%% File    : alter_table_1.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 30 Jan 2014 by Thomas Lindgren <>

%% EUnit tests for single-table migrations.

-module(alter_table_1).
-include_lib("eunit/include/eunit.hrl").

-import(schematool_table,
	[alter_table/1
	]).

-define(NODES, [a@localhost, b@localhost]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Change indexes [no changes to attributes, etc; see below]
%% - add index
%% - add indexes
%% - delete index
%% - delete indexes
%% - add and delete indexes
%%
%% - should know that index [1] is always there
%% - check handling of multi-field indexes too
%%   * if they exist??

add_index_test() ->
    Old_opts = 
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]}
	    ],
    New_opts =  
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]},
	     {index, [3]}
	    ],
    Tab = {wf_timer, Old_opts, New_opts},
    ?assertEqual([], alter_table(Tab)).

add_indexes_test() ->
    Old_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]}
	    ],
    New_opts = 
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]},
	     {index, [3]},
	     {index, [4]}
	    ],
    Tab = {wf_timer, Old_opts, New_opts},
    ?assertEqual([], alter_table(Tab)).

del_index_1_test() ->
    Old_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]}
	    ],
    New_opts = 
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES}
	    ],
    Tab = {wf_timer, Old_opts, New_opts},
    ?assertEqual([], alter_table(Tab)).

del_index_2_test() ->
    Old_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]},
	     {index, [3]}
	    ],
    New_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]}
	    ],
    Tab = {wf_timer, Old_opts, New_opts},
    ?assertEqual([], alter_table(Tab)).

del_indexes_test() ->
    Old_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]},
	     {index, [3]},
	     {index, [4]}
	    ],
    New_opts =
	    [{type, bag},
	     {attributes, [epoch_sec, id, event]},
	     {disc_copies, ?NODES},
	     {index, [2]}
	    ],
    Tab = {wf_timer, Old_opts, New_opts},
    ?assertEqual([], alter_table(Tab)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Change minor attributes
%%
%% - (storage_properties)
%% - should also test default values, e.g., option
%%   not given in Old or New yields correct value

access_mode_test() ->
    Old_opts =
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {access_mode, read_write}
	   ],
    New_opts = 
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {access_mode, read_only}
	   ],
    Tab = {wf_link, Old_opts, New_opts},
    ?assertEqual([{mnesia, change_table_access_mode, [wf_link, read_only]}], alter_table(Tab)).

load_order_test() ->
    Old_opts =
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {load_order, 0}
	   ],
    New_opts = 
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {load_order, 1}
	   ],
    Tab = {wf_link, Old_opts, New_opts},
    ?assertEqual([{mnesia, change_table_load_order, [wf_link, 1]}], alter_table(Tab)).

majority_test() ->
    Old_opts =
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {majority, false}
	   ],
    New_opts = 
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {majority, true}
	   ],
    Tab = {wf_link, Old_opts, New_opts},
    ?assertEqual([{mnesia, change_table_majority, [wf_link, true]}], alter_table(Tab)).

local_content_test() ->
    Old_opts =
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {local_content, false}
	   ],
    New_opts = 
	   [{type, set},
	    {attributes, [url, extras, id, event]},
	    {local_content, true}
	   ],
    Tab = {wf_link, Old_opts, New_opts},
    ?assertMatch([{error, _}], alter_table(Tab)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Change storage scheme
%%
%% A table can have MULTIPLE storage schemes, directed by what node
%% it is on. Ugh, but there we are.
%%  NB: not sure if mnesia handles all the possible transitions??
%%
%% 1. same storage scheme on all nodes
%% 2. different schemes on different nodes
%%    - all subsets of the 3 schemes
%%
%% tests:
%% - change storage scheme on all nodes
%% - change storage scheme on some nodes
%% - same scheme, add nodes
%% - same scheme, delete nodes
%% - same scheme, add and delete nodes, common nodes remain
%% - same scheme, add and delete nodes, no common nodes
%%
%% (could also do things like swap storage scheme)

same_scheme_change_all_test() ->
    exit(nyi).

same_scheme_change_some_test() ->
    exit(nyi).

same_scheme_add_nodes_test() ->
    exit(nyi).

same_scheme_del_nodes_test() ->
    exit(nyi).

same_scheme_add_del_common_nodes_test() ->
    exit(nyi).

same_scheme_add_del_nocommon_nodes_test() ->
    exit(nyi).

%% Can we generate and test all the possible subsets
%% instead?
%% 
%% We could also want to generate all N-partitions
%% of the input. (N = storage schemes.)
%%
%% Source and target:
%% - each node belongs to one of three schemes
%% - reverse index into 1-3 scheme lists
%% - test transition from Source to Target
%%   * umm can we generate the expected OUTCOME too?
%%
%% - for an assignment, select every non-empty
%%   subset of nodes and delete
%% 
%% - for an assignment, add nodes to one or more
%%   storage schemes
%%
%% - finally, do the add-delete nodes (common/nocommon) tests

gen_all_subsets(Nodes, Schemes) ->
    exit(nyi).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Change table type
%% - change type
%%   * set -> bag, set -> ord_set
%%   * ord_set -> bag, ord_set -> set
%%   * bag -> set, bag -> ord_set

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Change attributes and/or record name
%% - change record name [mix with others below]
%% - attrs unchanged
%% - add attrs
%% - del attrs
%% - permute attrs
%% - add, permute, del attrs
%% - transform attrs
%%   * once
%%   * many 
%%   * all attrs (in chain)
%% - transform record
%%   * once
%%   * many xforms
%% - indexes with attr changes (must reconstruct ix?)

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Multiple changes
%% - perform multiple table changes in correct order
%% - ...

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Fragmented tables
%% - ...

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Detect and handle default values correctly
%% - ....

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Detect errors and handle appropriately
%% - ... (not now)
