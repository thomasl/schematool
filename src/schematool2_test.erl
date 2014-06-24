%%% File    : schematool2_test.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  7 May 2014 by Thomas Lindgren <>

%% Test data for schematool2 tests
%%
%% Basically: a large number of table options, which
%% we then can use in tests.

-module(schematool2_test).
-export(
   [test/3]
  ).

test(Tab, Old_opts, New_opts) ->
    pp(Tab, Old_opts, New_opts, schematool2:alter_table(Tab, Old_opts, New_opts)).

pp(Tab, Old_opts, New_opts, Actions) ->
    io:format("%-----------\n"
	      "% ~p ~p ->\n"
	      "% ~p ~p\n", [Tab, Old_opts, Tab, New_opts]),
    lists:foreach(fun pp_action/1, Actions),
    io:format("\n%----------\n\n", []).

pp_action({comment, Str}) ->
    io:format("% ~s\n", [Str]);
pp_action({comment, Str, Xs}) ->
    io:format("% ~s\n", [io_lib:format(Str, Xs)]);
pp_action({M, F, As}) ->
    io:format(" ~p:~p~s\n", [M, F, fmt_args(As)]);
pp_action(Other) ->
    io:format("unknown: ~p\n", [Other]).

fmt_args([]) ->
    "()";
fmt_args([X|Xs]) ->
    ["(", io_lib:format("~p", [X])|fmt_rest_args(Xs)].

fmt_rest_args([]) ->
    ")";
fmt_rest_args([X|Xs]) ->
    [",", io_lib:format("~p", [X])|fmt_rest_args(Xs)].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Utility functions to mess around with schemas

update_opt(Fun, Tab, Schema) ->
    lists:map(
      fun({table, Tab, Opts}) ->
	      {table, Tab, lists:map(Fun, Opts)};
	 (Other) ->
	      Other
      end,
      Schema).

update_opt_all(Fun, Schema) ->
    lists:map(
      fun({table, Tab, Opts}) ->
	      {table, Tab, lists:map(Fun, Opts)};
	 (Other) ->
	      Other
      end,
      Schema).

update_opts(Fun, Tab, Schema) ->
    lists:map(
      fun({table, Tab, Opts}) ->
	      {table, Tab, Fun(Opts)};
	 (Other) ->
	      Other
      end,
      Schema).

update_all_opts(Fun, Schema) ->
    lists:map(
      fun({table, Tab, Opts}) ->
	      {table, Tab, Fun(Opts)};
	 (Other) ->
	      Other
      end,
      Schema).




