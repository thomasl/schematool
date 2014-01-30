%%% File    : alter_nodes_2.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 30 Jan 2014 by Thomas Lindgren <>

%% Tests of diff + alter_nodes. Note that output is a list
%% of instructions, not actual mnesia operations/side-effs.
%%  (The main difficulty here is checking the output!)

-module(alter_nodes_2).
-include_lib("eunit/include/eunit.hrl").

-import(schematool_nodes, 
	[diff/2,
	 alter_nodes/1,
	 alter_nodes/3
	]).

-define(nodes, [a@localhost, b@localhost, c@localhost]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - we're matching concrete data here, but SHOULD match relations
%%   such as "all of the following nodes are expanded before any
%%   replication and not after any replication"
%%   * we get unsorted lists as output, which causes spurious failures

nochange_test() ->
    Old = ?nodes,
    New = Old,
    Output = [],
    ?assertEqual(Output, diff_and_alter(Old, New)).

add_node_test() ->
    Added_Nodes = [d@localhost],
    Old = ?nodes,
    New = ?nodes ++ Added_Nodes,
    Output = [{expand, Added_Nodes},replicate],
    ?assertEqual(Output, diff_and_alter(Old, New)).
    
del_node_test() ->
    Old = ?nodes,
    New = [a@localhost, b@localhost],
    Output = [{contract, [c@localhost]}],
    ?assertEqual(Output, diff_and_alter(Old, New)).
    
add_del_node_common_test() ->
    Old = ?nodes,
    New = [a@localhost, b@localhost, d@localhost],
    Output = [{contract, [c@localhost]}, {expand, [d@localhost]}, replicate],
    ?assertEqual(Output, diff_and_alter(Old, New)).
    
add_del_node_nocommon_test() ->
    Old = ?nodes,
    New = [d@localhost, e@localhost],
    Output = [{expand, [d@localhost, e@localhost]}, replicate, {contract, ?nodes}],
    ?assertEqual(Output, diff_and_alter(Old, New)).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

diff_and_alter(Old, New) ->
    alter_nodes(diff(Old, New)).

