%%% File    : schematool_time.erl
%%% Author  : Thomas Lindgren <tl@Thomass-MacBook-Pro.local>
%%% Description : 
%%% Created : 

%% Wrapper for schematool time operations. We used to call erlang:now() previously.
%%
%% UNFINISHED - update everywhere to use new time ops and APIs

-module(schematool_time).
-export(
   [now/0
   ]).
%% UNFINISHED - these are not really time-related operations, just various 'unique keys'
-export(
   [
    binary_key/0,
    temp_table_name/1,
    opaque_backup_key/0
   ]
  ).
-compile({no_auto_import,[now/0]}).

opaque_backup_key() ->
    {A, B, C} = now(),
    Opaque = lists:flatten(
	       io_lib:format("schematooltmp-~p-~p-~p.bup",
			     [A,B,C])).

temp_table_name(Tab) ->
    {A, B, C} = now(),
    TmpName = 
	lists:flatten(
	  io_lib:format("~p_~p_~p_~p",
			[Tab, A, B, C])),
    list_to_atom(TmpName).

binary_key() ->
    Now = {_MSec, _Sec, USec} = now(),
    {{Y, M, D}, {HH, MM, SS}} = calendar:now_to_universal_time(Now),
    TZ = "Z",
    list_to_binary(
      io_lib:format("~4.4.0w-~2.2.0w-~2.2.0wT~2.2.0w:~2.2.0w:~2.2.0w.~w~s", 
		    [Y, M, D, HH, MM, SS, USec, TZ])
     ).

%% NB: This is for all recent versions of erlang, use erlang:now() for older ones.
%%  (older than OTP-18)

now() ->
    erlang:timestamp().

