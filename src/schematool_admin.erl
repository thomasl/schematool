%%% File    : schematool_admin.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 21 Mar 2014 by Thomas Lindgren <>

%% Experimental, for schema management purposes
%%
%% TERMINOLOGY
%% ==========
%% - a SCHEMA is a model of a mnesia database
%% - we can load CANDIDATE SCHEMAS into the db
%%   * there can be any number of candidate schemas
%% - at each time, there is zero or one CURRENT SCHEMA
%%   * no schema just at the start (if even then)
%%   * technically, the current schema is a candidate schema
%% - we can unload any candidate except the current schema
%% - we can MIGRATE from one schema to another
%%   * migration reconfigures the mnesia db
%%   * migration can fail, in which case it is ROLLED BACK
%%   * the admin chooses a new schema and triggers migration
%%
%% STATUS:
%% - seems to work, at least up to migrations
%% - add functions so we can manage from command line / Make
%%   * in particular, key name of initial schema ...)
%%     (or do we add the version field again?)
%%   * 

-module(schematool_admin).
-export(
   [install/1,
    candidate_schemas/0,
    load_schema_def/1,
    load_schema_definition/1,
    current_schema/0,
    def/1,
    definition/1,
    migrate_to/1,
    delete_schema/1,
    tables_of/1
   ]
  ).
-include("schematool.hrl").

%% (Note: schematool_exists() unnecessary if we done wait_for_mnesia before)

-define(txn(Expr),
   atomic(mnesia:transaction(fun() -> Expr end))).

-define(txn_or_err(Expr, ErrFun),
   atomic(mnesia:transaction(fun() -> Expr end), 
	  ErrFun)).

-define(schematool_tables, [schematool_info, schematool_changelog, schematool_config]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Run this to initialize schematool (only) on the specified nodes.
%%
%% UNFINISHED
%% - if schema exists or schematool exists, we get a confusing fail
%% - schematool tables should really also use a schema
%%   * allow migration
%%   * change nodes involved
%%   * need bootstrap procedure

install(Nodes) ->
    application:load(mnesia),
    ping_all_nodes(Nodes),
    case mnesia:create_schema(Nodes) of
	{error, Rsn} ->
	    io:format("Unable to create fresh schema for ~p: ~p\n", [Nodes, Rsn]);
	ok ->
	    %% Now create the tables and do whatever other setup
	    %% is needed
	    start_mnesia_all_nodes(Nodes),
	    create_schematool_tables(Nodes),
	    %% printout
	    mnesia:info()
    end.

%% Make sure all specified nodes are connected to this one.

ping_all_nodes(Nodes) ->
    Failures = 
	lists:foldl(
	  fun(N, Acc) ->
		  case net_adm:ping(N) of
		      pong ->
			  Acc;
		      pang ->
			  io:format("Node ~p unreachable\n", [N]),
			  Acc+1
		  end
	  end,
	  0,
	  Nodes),
    if
	Failures == 0 ->
	    ok;
	true ->
	    exit(some_nodes_unreachable)
    end.

%% Start mnesia on all nodes
%%
%% - no checking of return value, is that good ...?
%% - should we use rpc:multicall instead and look over the
%%   results en masse?

start_mnesia_all_nodes(Ns) ->
    lists:foreach(
      fun(N) ->
	      io:format(
		"start mnesia ~p -> ~p\n", 
		[N, rpc:call(N, application, ensure_all_started, [mnesia])]
	       )
      end,
      Ns).

%% Create the schematool tables
%%
%% Would be better to create these in a loop, but the record_info()
%% crushes that hope. 
%%
%% - schematool should have a schema (but we then need to bootstrap
%%   when creating)

create_schematool_tables(Nodes) ->
    io:format("Create schematool table -> ~p\n", 
	      [mnesia:create_table(schematool_info, 
				   [{type, ordered_set}, 
				    {disc_copies, Nodes},
				    {attributes, 
				     record_info(fields, schematool_info)}])]),
    io:format("Create schematool changelog -> ~p\n", 
	      [mnesia:create_table(schematool_changelog, 
				   [{type, ordered_set}, 
				    {disc_copies, Nodes},
				    {attributes, 
				     record_info(fields, schematool_changelog)}])]),
    io:format("Create schematool changelog -> ~p\n", 
	      [mnesia:create_table(schematool_config, 
				   [{type, ordered_set}, 
				    {disc_copies, Nodes},
				    {attributes, [key, value]}])]),
    done.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Use this first in all management operations (except when creating the schema), 
%% or the results may be puzzling.
%%
%% UNFINISHED
%% - the MaxWait constant is icky

wait_for_mnesia() ->
    {ok, _} = application:ensure_all_started(mnesia),
    MaxWait = 5000,
    ok = mnesia:wait_for_tables(schematool:schematool_tables(), MaxWait).

%% Used to unwrap transaction results. Can optionally do something
%% with an error (like printout or exit).

atomic(X) ->
    atomic(X, fun(Err) -> Err end).

atomic({atomic, Res}, Fail) ->
    Res;
atomic(Err, Fail) ->
    Fail(Err).

%% Used inside txn to check that schematool has been installed.
%% Aborts if schematool_info does not exist
%%
%% NOTE: if you've done wait_for_mnesia() before, then this will
%%  always succeed. Seldom useful.

schematool_exists() ->
    mnesia:table_info(schematool_info, attributes).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Return all the currently loaded schemas (as keys!)

candidate_schemas() ->
    wait_for_mnesia(),
    ?txn(
       mnesia:foldr(
	 fun(#schematool_info{datetime=Key} = Schema, Acc) ->
		 [Key|Acc]
	 end,
	 [],
	 schematool_info)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Load the schema. 
%%
%% NB: File can be an atom or a list. See consult/1 for what files
%% we try.

load_schema_file(File) ->
    Schema = consult(File),
    load_schema_definition(Schema).

load_schema_def(Schema) ->
    load_schema_definition(Schema).

load_schema_definition(Schema) ->
    wait_for_mnesia(),
    ?txn_or_err(
       write_schema_info(Schema),
       fun(Err) ->
	       io:format("Error when loading schema: ~p\n"
			 "Has the node been initialized?\n",
			 [Err])
       end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Lookup the current schema (key) in use. Fails if no schema installed.

current_schema() ->
    wait_for_mnesia(),
    ?txn_or_err(
       begin
	   [#schematool_config{value=Curr}] = mnesia:read(schematool_config, current_schema),
	   Curr
       end,
       fun(Err) ->
	      not_found
       end).

%% Delete a candidate schema. Fails if schema does not exist
%% or schema is current schema.
%%
%% NB: What if we WANT TO delete the current schema? Any valid
%%  situation for that?

delete_schema(Schema_key) ->
    wait_for_mnesia(),
    DT = make_key(),
    Curr_key = current_schema(),
    if
	Schema_key == Curr_key ->
	    exit({cannot_delete_current_schema, Curr_key});
	true ->
	    ?txn(begin
		     mnesia:delete({schematool_info, Schema_key}),
		     mnesia:write(
		       #schematool_changelog{datetime=DT,
					     action={delete, Schema_key}})
		 end)
    end.

%% When a schema is loaded, it gets a key which is the universal (UTC) datetime of
%% the load. We prettyprint it in ISO XXXX format to make it less awful.

make_key() ->
    {{Y, M, D}, {HH, MM, SS}} = calendar:universal_time(),
    TZ = "Z",
    list_to_binary(
      io_lib:format("~4.4.0w-~2.2.0w-~2.2.0wT~2.2.0w:~2.2.0w:~2.2.0w~s\n", 
		    [Y, M, D, HH, MM, SS, TZ])
     ).

%% Returns the schema definition of the schema key.

def(Schema_key) ->
    definition(Schema_key).

definition(Schema_key) ->
    wait_for_mnesia(),
    ?txn_or_err(
       begin
	   [#schematool_info{schema=Schema}] = mnesia:read({schematool_info, Schema_key}),
	   Schema
       end,
       fun(Err) ->
	       io:format("Schema not found: ~p\n", [Schema_key]),
	       exit(Err)
       end).

%% Schema_key indicates a candidate schema. Migrate the database
%% from current schema to that one.
%%
%% If schema_key is the same as the current one, done
%% If schema_key is not for a candidate, fail
%% Otherwise, "try to do it".
%%
%% NB: somewhat awkward, e.g. does a number of waits for mnesia, but I
%% think that's not the performance critical part. (The actual migration
%% is the main work.)

migrate_to(Schema_key) ->
    wait_for_mnesia(),
    case current_schema() of
	not_found ->
	    New_schema = definition(Schema_key),
	    install_schema(New_schema),
	    ?txn(set_current_schema(Schema_key));
	Curr_key ->
	    if
		Curr_key == Schema_key ->
		    io:format("Schema ~p is already current\n",
			      [Curr_key]),
		    ok;
		true ->
		    %% migrate Curr_key -> Schema_key
		    migrate_schema(Curr_key, Schema_key),
		    ?txn(set_current_schema(Schema_key))
	    end
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Temporary - refactor to other module
%% Install a schema in a fresh system (no current_schema)
%%
%% UNFINISHED
%% - should return something useful, e.g if tabs couldn't be created

install_schema(Schema) ->
    wait_for_mnesia(),
    lists:foreach(
      fun({table, Tab, Opts0}) ->
	      Opts = schematool_options:without(Opts0),
	      io:format("Create table ~p ~p -> ~p\n",
			[Tab, Opts,
			 mnesia:create_table(Tab, Opts)]);
	 (_Other) ->
	      %% Skip other options
	      ok
      end,
      Schema).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Temporary - refactor to other module
%%
%% Migrate from old schema to new schema.

migrate_schema(Old_key, New_key) ->
    Old = definition(Old_key),
    New = definition(New_key),
    Tabs = tables_of(Old_key),
    Actions = schematool:diff(Old, New),
    schematool_backup:migrate(Tabs, migration_fun(Actions)).

%% This one should interpret the Actions to complete the migration,
%% print what's going on and fail/exit if you can't do it
%%  (That will trigger rollback.)

migration_fun(Actions) ->
    fun() ->
	    perform_actions(Actions)
    end.

%% Perform the migration actions
%%
%% - should also print comments appropriately, etc
%% - match return values?

perform_actions(Actions) ->
    lists:foreach(
      fun({M, F, Args}) ->
	      Res = (catch erlang:apply(M, F, Args)),
	      io:format("~p:~p ~p -> ~p\n",
			[M, F, Args, Res]),
	      case Res of
		  {'EXIT', Rsn} ->
		      exit(Rsn);
		  _ ->
		      Res
	      end
      end,
      Actions).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

tables_of(Schema_key) ->
    Schema = def(Schema_key),
    ?schematool_tables ++ [ Tab || {table, Tab, _Opts} <- Schema ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Use this to write the newly loaded schema. Returns the schema key.
%%
%% Run in txn.

write_schema_info(Schema) ->
    Datetime = make_key(),
    write_schema_info(Datetime, Schema).

write_schema_info(Datetime, Schema) ->
    mnesia:write(#schematool_info{datetime=Datetime,
				  schema=Schema}),
    mnesia:write(#schematool_changelog{datetime=Datetime,
				       action={create, Datetime}}),
    Datetime.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Strictly internal functions (or you risk messing it all up). in txn.

get_current_schema() ->
    case mnesia:read(schematool_config, current_schema) of
	[] ->
	    not_found;
	[#schematool_config{value=Schema_key}] ->
	    {found, Schema_key}
    end.

set_current_schema(Schema_key) ->
    mnesia:write(#schematool_config{key=current_schema,
				    value=Schema_key}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% The default schema file of module is module.schema.term

consult(F0) ->
    F = filename(F0),
    consult_one([F, F++".schema.term", F++".term"]).

filename(A) when is_atom(A) ->
    atom_to_list(A);
filename(Lst) when is_list(Lst) ->
    binary_to_list(list_to_binary(Lst)).

consult_one([F|Fs]) ->
    case catch consult_schema(F) of
	{'EXIT', _Rsn} ->
	    consult_one(Fs);
	Res ->
	    Res
    end;
consult_one([]) ->
    %% or something more useful?
    exit({error, enoent}).

%% Read the schema file if possible.  Only permit one schema in a
%% file.

consult_schema(F) ->
    case file:consult(F) of
	{ok, [Schema]} ->
	    Schema;
	{ok, [Schema|_]} ->
	    exit({multiple_schemas_in_file, F});
	Err ->
	    exit(Err)
    end.

