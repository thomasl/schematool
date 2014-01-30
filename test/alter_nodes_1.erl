%%% File    : alter_nodes_1.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 30 Jan 2014 by Thomas Lindgren <>

-module(alter_nodes_1).
-include_lib("eunit/include/eunit.hrl").

-import(schematool_nodes, 
	[diff/2,
	 alter_nodes/3
	]).

-define(nodes, [a@localhost, b@localhost, c@localhost]).

%% (Need to test with sname too? Mixed sname/name?)

add_diff_test() ->
    Add = [d@localhost],
    Rem = ?nodes,
    Del = [],
    ?assertEqual({Add, Rem, Del}, sorted_diff(?nodes, ?nodes ++ [d@localhost])).

del_diff_test() ->
    Add = [],
    Rem = [a@localhost, b@localhost],
    Del = [c@localhost],
    ?assertEqual({Add, Rem, Del}, sorted_diff(?nodes, ?nodes -- [c@localhost])).

add_del_diff_common_test() ->
    Add = [d@localhost],
    Rem = [a@localhost, b@localhost],
    Del = [c@localhost],
    ?assertEqual({Add, Rem, Del}, sorted_diff(?nodes, [a@localhost, b@localhost, d@localhost])).

add_del_diff_no_common_test() ->
    Add = [d@localhost, e@localhost],
    Rem = [],
    Del = ?nodes,
    ?assertEqual({Add, Rem, Del}, sorted_diff(?nodes, [d@localhost, e@localhost])).

sorted_diff(Old, New) ->
    io:format("Old = ~p, New = ~p\n", [Old, New]),
    {A, R, D} = schematool_nodes:diff(Old, New),
    io:format("A = ~p, R = ~p, D = ~p\n", [A, R, D]),
    {lists:sort(A), lists:sort(R), lists:sort(D)}.

