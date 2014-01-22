%%% File    : schematool.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 22 Jan 2014 by Thomas Lindgren <>

%% NOTE: to run schematool, see ../bin/schematool.pl
%%
%% This module is the start of schema processing once we
%% have the schema module compiled and generated.

%% STATUS
%% - very early
%% - test invoking it from command line

-module(schematool).
-export(
   [module/1,
    create_schema/1
   ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

create_schema(M) ->
    Schema = module(M),
    cr_schema(Schema).

module([M]) ->
    %% (for use on command line)
    module(M);
module(M) when is_atom(M) ->
   case catch M:schema() of
       {'EXIT', Rsn} ->
	   io:format("schematool: EXIT ~p\n", [Rsn]);
       Schema ->
	   Schema
   end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Initialize the current node using the defined schema.
%%
%% Note that mnesia is a little finicky. We need to load mnesia
%% before schema is created, but not start it. We need to start
%% it before creating the tables.
%% 
%% UNFINISHED
%% - no error checking, e.g., db already exists
%% - maybe application:stop(mnesia)

cr_schema(Schema) ->
    application:load(mnesia),
    Nodes = get_value(nodes, Schema, [node()]),
    case mnesia:create_schema(Nodes) of
	{error, Rsn} ->
	    io:format("Unable to create fresh schema ~p: ~p\n", [Nodes, Rsn]);
	ok ->
	    %% Now create the tables and do whatever other setup
	    %% is needed
	    application:start(mnesia),
	    lists:foreach(
	      fun({table, Tab, Opts}) ->
		      mnesia:create_table(Tab, Opts);
		 (Other) ->
		      %% skip the others
		      ok
	      end,
	      Schema),
	    %% mnesia:info(), %% show status
	    %% application:stop(mnesia),
	    ok
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Diff schema1 and schema2

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% UNFINISHED
%% - (Should Dflt be a closure, evaluated only if not found?
%%   If so, return Dflt() instead of Dflt.)

get_value(Key, [{Key, Val}|Xs], Dflt) ->
    Val;
get_value(Key, [_|Xs], Dflt) ->
    get_value(Key, Xs, Dflt);
get_value(Key, [], Dflt) ->
    Dflt.

