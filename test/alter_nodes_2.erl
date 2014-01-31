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
	 alter_nodes/2,
	 alter_nodes/4
	]).

-define(nodes, [a@localhost, b@localhost, c@localhost]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - output data have changed, so these tests must be updated!
%% - shouldn't match concrete data but PROPERTIES of output
%%   * how to do that?

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
    Schema = undef_schema_module, 
    alter_nodes(Schema, diff(Old, New)).

