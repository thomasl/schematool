%%% File    : schematool.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 22 Jan 2014 by Thomas Lindgren <>

%% NOTE: to run schematool, see ../bin/schematool.pl
%%
%% This module is the start of schema processing once we
%% have the schema module compiled and generated.
%%
%% Update the schema() type family appropriately when
%% adding new options.
%%
%% Architectural notes:
%% - we should PERHAPS have a god-node containing
%%   the schema and all related info (e.g., all versions), 
%%   then propagate the create/migrate info to all work-nodes
%%   * we then avoid trouble with doing dangerous ops
%%     like mnesia:delete_schema on possibly every node
%% - schematool should be configurable to do a BACKUP
%%   before any dangerous/data-lossy migration
%%   * how to detect this? [mark ops as dangerous?]
%%   * how to do this?
%%     - how to be efficient about it?
%%   * where to insert the backup operation?
%%   * where to configure that backup is desired?
%%   * how to trigger rollback to backup?
%%   * how to implement full rollback?
%%   (* can/should we use checkpoints instead?)
%%
%% Here's how it PROBABLY should be:
%% - store the current schema in mnesia (table:schematool_schema)
%%   * as "datetime -> schemablob" + 'current'
%% - on an upgrade, diff the new and current schema to get
%%   migration instructions
%%   * script should detect init vs upgrade
%% - generate upgrade instructions
%%   * later on: run them automatically
%%
%% Advanced topics:
%% - qlc
%% - automatic accessors (using indexes etc, in a
%%   single consistent notation)
%% - discless nodes
%% - merging independent copies of mnesia
%%   (may fail if same table created independently
%%   on two nodes, do the song-and-dance)

%% STATUS
%% - still early days, but (a) creates schema from spec
%%   and (b) generates reasonable migration plan from
%%   schema diffs (diff/2)
%% - schema versions are kept in schematool_info
%%   and the changes are kept in schematool_changelog
%% - find current schema by looking at changelog
%%
%% UNFINISHED
%% - loaded schemas with same vsn => warning
%%   {vsn, "foo"}
%% - derive schema by checking mnesia
%% - verify schema (derived vs latest)
%% - repair schema: diff
%% - nodes should (perhaps) be specified as
%%    {NodeName, Startup}
%%   e.g.,
%%    {'a@hostname', "erl -pa ../ebin -name ..."}
%%   this way we can ensure that they're all started when
%%   creating the schema
%%   * also, current node doesn't have to be a member

-module(schematool).
-export(
   [module/1,
    create_schema/1,
    load_schema/1,
    diff/2,
    diff_modules/2
   ]).

-export([view_schemas/0,
	 schema_keys/0,
	 get_schema/1,
	 delete_schema/1
	]).

-include("schematool.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-spec schematool:module(module()) -> any().
-spec schematool:create_schema(schema()) -> any().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-export_type([schema/0]).

-type schema() :: [schema_item()].   %% The main type of interest

-type schema_item() :: 
     {nodes, [node_name()]}
   | {table, table_name(), [table_opt()]}.

-type node_name() :: node().
-type table_name() :: atom().
-type table_opt() :: term().   %% could be more precise

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Typical usage:
%% $ (generate schemamod using schematool)
%% $ $SCHEMATOOL_BIN/create-schema.pl --schema foo --node a@localhost 
%%
%% where
%%   foo is usually a module (name) created by schematool
%%
%% After creating the schema, start the node with the given nodename:
%%
%% $ erl -name a@localhost
%%
%% (+ add your other erlang options of course)
%%
%% Note that we only use long names, -sname verboten.

create_schema(M) ->
    Schema = module(M),
    io:format("Schema: ~p\n", [Schema]),
    io:format("Creating ...\n", []),
    cr_schema(Schema).

%% Given the module or filename, retrieve the schema or exit.
%%
%% Note: we try to handle both when invoked from cmdline and
%% inside a program, and when given atoms or strings.

module([M]) when is_atom(M) ->
    module(M);
module(M) when is_atom(M) ->
    File = filename(M),
    consult(File);
module(File) when length(File) >= 0 ->
    %% ASSUME string, try to open variants
    consult_one([File, File++".schema.term", File ++ ".term"]);
module(M) ->
    exit({module_or_filename_not_handled, M}).

filename(A) when is_atom(A) ->
    lists:flatten([atom_to_list(A), ".schema.term"]).

consult_one([F|Fs]) ->
    case catch consult(F) of
	{'EXIT', Rsn} ->
	    consult_one(Fs);
	Res ->
	    Res
    end;
consult_one([]) ->
    %% or something more useful?
    exit({error, enoent}).

consult(F) ->
    case file:consult(F) of
	{ok, [Schema]} ->
	    Schema;
	{ok, [Schema|_]} ->
	    exit({multiple_schemas_in_file, F});
	Err ->
	    exit(Err)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Initialize the current node using the defined schema.
%%
%% Note that mnesia is a little finicky. We need to load mnesia
%% before schema is created, but not start it. We need to start
%% mnesia before creating the tables though.
%% 
%% If the current node is not part of the schema, then skip creating
%% schema or tables. This is helpful when we just want to run the same
%% scripts for all nodes regardless of whether they run mnesia or not.
%%
%% UNFINISHED
%% - no error checking, e.g., db already exists
%% - maybe application:stop(mnesia) afterwards?
%% - more printouts
%% - run creation in txn and fail if some part fails

cr_schema(Schema) ->
    application:load(mnesia),
    Nodes = get_nodes(Schema),
    if
	Nodes == [] ->
	    io:format("No nodes found, stopping\n",[]);
	true ->
	    require_all_nodes_up(Nodes),
	    io:format("- schema for nodes ~p\n", [Nodes]),
	    case lists:member(node(), Nodes) of
		true ->
		    case mnesia:create_schema(Nodes) of
			{error, Rsn} ->
			    io:format("Unable to create fresh schema ~p: ~p\n", [Nodes, Rsn]);
			ok ->
			    %% Now create the tables and do whatever other setup
			    %% is needed
			    start_mnesia_all_nodes(Nodes),
			    create_schematool_info(Nodes, Schema),
			    lists:foreach(
			      fun({table, Tab, Opts0}) ->
				      %% check return value
				      Opts = schematool_options:without(Opts0),
				      io:format("Create table ~p ~p-> ~p\n", 
						[Tab, Opts, mnesia:create_table(Tab, Opts)]);
				 (Other) ->
				      %% skip non-table items
				      ok
			      end,
			      Schema),
			    mnesia:info(), %% show status, a bit messy
			    %% application:stop(mnesia),
			    ok
		    end;
		false ->
		    %% Current node is NOT part of schema,
		    %% do not create tables
		    %% - warning or log this? verbose option, could
		    %%   be annoying to run in scripts otherwise
		    io:format("- current node ~p is not among mnesia nodes ~p, stop\n", [node(), Nodes]),
		    ok
	    end
    end.

%% Ensure that all nodes in Ns are connected or exit.
%%
%% NB: would be nice to also have a tool for STARTING the nodes on all the remote hosts
%%  sigh

require_all_nodes_up(Ns0) ->
    Ns = lists:sort(Ns0),
    Nodes = lists:sort(nodes()),
    case Ns == Nodes of
	true ->
	    ok;
	false ->
	    %% ping all nodes in Ns but not in nodes()
	    %% - if ping fails, the whole thing fails
	    lists:foreach(
	      fun(N) ->
		      case lists:member(N, Nodes) of
			  true ->
			      ok;
			  false ->
			      case net_adm:ping(N) of
				  pong ->
				      ok;
				  pang ->
				      exit({unable_to_connect_to_node, N})
			      end
		      end
	      end,
	      Ns)
    end.

%% 

get_nodes(Schema) ->
    [ node_name_of(N) || N <- proplists:get_value(nodes, Schema, [node()]) ].

%% OBSOLETE
%% - only Node = atom() permitted for now

node_name_of({Node, _Startup}) ->
    Node;
node_name_of(Node) when is_atom(Node) ->
    Node.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Start nodes locally
%%
%% Intended for testing: start nodes locally
%%
%% Example:
%%   start_local_nodes([a,b], Host, "erl -detached -pa ../ebin -name ~p").
%%
%% - be careful ...
%% - can be used to start nodes remotely with the right ssh or whatever
%%   * I think node starting should be done by epmd ...
%%     = start epmd on all hosts
%%     = config to run as appropriate user in appropriate place
%%       (in a jail or something) [associated with cookie?]
%%     = start nodes with os:cmd when so ordered by someone

start_local_nodes(Startup, SNames) ->
    %% PORTABILITY: this works on OS/X (Mavericks)
    Host = lists:flatten(os:cmd("hostname -s")),
    start_local_nodes(SNames, Host, Startup).

start_local_nodes(Startup, Host, SNames) ->
    lists:foreach(
      fun(SName) ->
	      %% 1/ ping node, if up we're done
	      %% 2/ 
	      %% 
	      %% (a bit wasteful to construct this atom, but
	      %% I want to make the format string natural)
	      Name = list_to_atom(
		       lists:flatten(
			 io_lib:format("~p@~p", [SName, Host]))),
	      Cmd = lists:flatten(io_lib:format(Startup, [Name])),
	      io:format("'~s' -> ~p\n", [Cmd, os:cmd(Cmd)])
      end,
      SNames).

%% Stop node N.
%%
%% Mostly intended for testing, but can be used whenever

stop_node(N) ->
    rpc:call(N, init, stop, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Start mnesia on all nodes
%%
%% - no checking of return value, is that good?
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Store the current and previous schema versions.
%%
%% The key is {{YYYY, MM, DD}, {H, M, S}}.
%%
%% UNFINISHED
%% - how do we UPGRADE/MIGRATE these hidden tables?
%% - use erlang:now() instead of datetime?
%%   + store datetime as readable string/binary field
%% - should we wrap this in a txn?

-define(schematool_info, schematool_info).
-define(schematool_changelog, schematool_changelog).

create_schematool_info(Nodes, Schema) ->
    Datetime = calendar:universal_time(),
    create_schematool_info(Datetime, Nodes, Schema).

%% Create and store the relevant schematool info for later use.
%% Table is currently stored on all nodes.

create_schematool_info(Datetime, Nodes, Schema) ->
    io:format("Create schematool table -> ~p\n", 
	      [mnesia:create_table(?schematool_info, 
				   [{type, ordered_set}, 
				    {disc_copies, Nodes},
				    {attributes, 
				     record_info(fields, schematool_info)}])]),
    io:format("Create schematool changelog -> ~p\n", 
	      [mnesia:create_table(?schematool_changelog, 
				   [{type, ordered_set}, 
				    {disc_copies, Nodes},
				    {attributes, 
				     record_info(fields, schematool_changelog)}])]),
    io:format("Write schema info -> ~p\n",
	      [mnesia:transaction(
		 fun() ->
			 add_schema_info(Datetime, Schema)
		 end)
	       ]).

%% Return current schema definition (as a #schemainfo record),
%% or an error.
%%
%% Can be used for diff purposes.
%%
%% UNFINISHED
%% - traverse schematool_changelog for this instead!

current_schema() ->
    atomic(
      mnesia:transaction(
	fun() ->
		Key = mnesia:last(schematool_info),
		[Rec] = mnesia:read(schematool_info, Key),
		Rec
	end)).

%% - would be nice if we could retry txn as well

atomic({atomic, Res}) ->
    Res;
atomic(Err) ->
    Err.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Load a new schema from module M. Note: this does NOT upgrade/migrate
%% yet, and MAY be problematic (since it will show up as the latest schema).
%%
%% Thus, mainly intended for TESTING at this time.
%%
%% [schematool should mark the node with the currently active schema,
%% with 0,1 or more schema versions loaded after it. How should those
%% schemas be treated? They are basically tentative, but probably don't
%% form a chain; perhaps unrelated.]

load_schema(M) ->
    Schema = module(M),
    application:ensure_all_started(mnesia),
    case mnesia:transaction(
	   fun() ->
		   %% will abort if schematool_info does not exist
		   mnesia:table_info(schematool_info, attributes),
		   add_schema_info(Schema)
	   end) of
	{atomic, Res} ->
	    Res;
	Err ->
	    io:format("Error when loading schema. "
		      "Has the node been initialized?\n", 
		      []),
	    Err
    end.

%% Add a new schema definition to schematool_info. Note
%% that this is just one part of migrating the schema.
%%
%% We currently use Datetime as the key, but should
%% perhaps use erlang:now() instead. The datetime
%% should always be universaltime() to avoid timezone issues.
%% (See add_schema_info/1.)
%% 
%% Perhaps: also store datetime as formatted binary field for
%% convenience. E.g., <<"2014-01-01 23:59:59Z">> or something.

add_schema_info(Schema) ->
    Datetime = calendar:universal_time(),
    add_schema_info(Datetime, Schema).

%% (Note: if Datetime is not given in universaltime, 
%% things get problematically unsorted.)
%%
%% UNFINISHED
%% - undo: should we store {create, Key, Schema} ...?
%%   that permits us to undo, at a cost of much more
%%   garbage

add_schema_info(Datetime, Schema) ->
    mnesia:write(#schematool_info{datetime=Datetime,
				  schema=Schema}),
    mnesia:write(#schematool_changelog{datetime=Datetime,
				       action={create, Datetime}}).

%% Given a schema key, read the schema.

get_schema(Key) ->
    atomic(
      mnesia:transaction(
	fun() ->
		[#schematool_info{schema=S}] = 
		    mnesia:read(schematool_info, Key),
		S
	end)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Return all the schemas currently stored. Mostly intended for
%% troubleshooting or testing.
%%
%% - badly named? "view_" ...
%% - should we have a schema prettyprinter? ~p might be enough

view_schemas() ->
    application:ensure_all_started(mnesia),
    atomic(
      mnesia:transaction(
	fun() ->
		mnesia:foldr(
		  fun(#schematool_info{} = Schema, Acc) ->
			  [Schema|Acc]
		  end,
		  [],
		  schematool_info)
	end)).

schema_keys() ->
    application:ensure_all_started(mnesia),
    atomic(
      mnesia:transaction(
	fun() ->
		mnesia:foldr(
		  fun(#schematool_info{datetime=Key}, Acc) ->
			  [Key|Acc]
		  end,
		  [],
		  schematool_info)
	end)).

schema_actions() ->
    application:ensure_all_started(mnesia),
    atomic(
      mnesia:transaction(
	fun() ->
		mnesia:foldr(
		  fun(#schematool_changelog{datetime=Key, action=Act}, Acc) ->
			  [{Key, Act}|Acc]
		  end,
		  [],
		  schematool_changelog)
	end)).

%% Note: this is a DANGEROUS OPERATION, since you can e.g., delete the
%% currently used schema and so confuse schematool and yourself. Be
%% careful.
%%
%% - should we store {delete, Key, Schema} for undo? :-)

delete_schema(Key) ->
    application:ensure_all_started(mnesia),
    Datetime = calendar:universal_time(),
    atomic(
      mnesia:transaction(
	fun() ->
		mnesia:delete({schematool_info, Key}),
		mnesia:write(#schematool_changelog{datetime=Datetime,
						   action={delete, Key}})
	end)).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Diff schema1 and schema2 (old vs new). Foundation for
%% performing upgrade/downgrade/...
%% 
%% Notational issue: how to write table transforms?
%% - as a separate key?
%% - associated with table opts?
%% - something else?
%% - helpers to allow use of new and old record attrs
%%
%% UNFINISHED
%% - output needs to be massaged

%% diff of schemas in modules M0, M1

diff_modules(M0, M1) ->
    S0 = M0:schema(),
    S1 = M1:schema(),
    diff(S0, S1).

%% The following can only run in a node with
%% active schematool (= mnesia + schematool_info table)

diff_current(M1) ->
    S1 = M1:schema(),
    #schematool_info{schema=S0} = current_schema(),
    diff(S0, S1).

%% diff of schemas S0, S1

diff(S0, S1) ->
    N = nodediff(S0, S1),
    Ts = tablesdiff(S0, S1),
    cons(nodes, N, cons(tables, Ts, [])).

%% Diff of nodes in schemas S0, S1
%%
%% Returns a list of operations at the moment.
%%
%% UNFINISHED
%% - operations MUST NOT run without considering
%%   the table migrations (which may need to be done intermixed with
%%   node migrations)
%%   * the common case of no changes will yield an empty list though

nodediff(S0, S1) ->
    N0 = get_nodes(S0),
    N1 = get_nodes(S1),
    diff_ops(N0, N1).

%% Generate list of operations to add/delete nodes.
%%
%% NOTE: this is NOT YET SAFE to use. In order to migrate tables
%% safely, we CANNOT add/delete nodes independently of the table
%% operations!

diff_ops(Old_lst, New_lst) ->
    {Added, Remaining, Deleted} = schematool_nodes:diff(Old_lst, New_lst),
    Ops =
	[ {schematool_helper, add_node, [Node]} || Node <- Added ] ++
	[ {schematool_helper, delete_node, [Node]} || Node <- Deleted ],
    case Ops of
	[] ->
	    [];
	_ ->
	    [ {warning, <<"UNFINISHED - add/delete nodes must consider table operations">>} ] ++ Ops
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Collect tables that have been added, deleted or changed.
%%
%% Returns a list of actions that need to be performed to
%% migrate from S0 to S1.
%%
%% Note: if N0 or N1 is just a single node, we do not have to
%%  use the helper functions, just create/delete tables directly
%%
%% Note: interaction with add/delete nodes? 
%% - (and changing fragments?)
%%
%% STATUS
%% - the generated actions looked pretty good when moving
%%   from schema A to schema B and back again
%%   * e.g., recreated table, changed layout back, etc
%%   * like Rails migrations, dropping fields loses data,
%%     but this is up to the user at the moment
%%     
%% UNFINISHED
%% - not sure if ordering of actions is good
%% - the actions need to be revised
%%   * create table with correct options
%% - warning if attributes lost/dropped?
%% - might be nice to have notation handling
%%   renaming a field back and forth automatically
%%   (e.g., field id is copied to old_id and updated;
%%   on down migration, auto-copy old_id to id again
%%   to restore?) (except new records might not have
%%   a valid old_id ...)

tablesdiff(S0, S1) ->
    %% Select tables from schema def, since it's a list
    %% at the moment, we just keep them
    T0 = S0,
    T1 = S1,
    N0 = get_nodes(S0),
    N1 = get_nodes(S1),
    Added = [ Table || {table, Tab, _} = Table <- S1,
		       not table_defined(Tab, S0) ],
    Deleted = [ Tab || {table, Tab, _} <- S0,
		       not table_defined(Tab, S1) ],
    Changed = tables_changed(T0, T1),
    lists:flatten(
      [ {schematool_helper, create_table_all_nodes, [N1, Add, Opts]} 
	|| {table, Add, Opts} <- Added ] ++
      [ {schematool_helper, delete_table_all_nodes, [N0, Del]} 
	|| Del <- Deleted ] ++
      [ schematool_table:alter_table(Chg) || Chg <- Changed ]
     ).

%% Lookup the tables found in both lists of tables and check
%% if their options field differ. If so, return both. [Added
%% or deleted tables are handled elsewhere.]
%%
%% Non-table definitions are skipped.

tables_changed([{table, TabName, OldOpts}|Ts], Tabs) ->
    case table_def(TabName, Tabs) of
	not_found ->
	    tables_changed(Ts, Tabs);
	{found, {table, _TabName, NewOpts}} ->
	    if
		OldOpts == NewOpts ->
		    tables_changed(Ts, Tabs);
		true ->
		    [{TabName, OldOpts, NewOpts}|tables_changed(Ts, Tabs)]
	    end
    end;
tables_changed([_Other|Ts], Tabs) ->
    tables_changed(Ts, Tabs);
tables_changed([], _Tabs) ->
    [].

%% Return name of table definition.

table_name({table, Name, _Opts}) ->
    Name.

%% Find definition of table in schema, returns
%%  {found, Val} | not_found.

table_def(Name0, [{table, Name, _Opts}=Tab|Xs]) ->
    if
	Name0 == Name ->
	    {found, Tab};
	true ->
	    table_def(Name0, Xs)
    end;
table_def(Name, [_|Xs]) ->
    table_def(Name, Xs);
table_def(_Name, []) ->
    not_found.

%% Is table Name0 defined in schema? Returns bool()
		
table_defined(Name0, [{table, Name, _Opts}|Xs]) ->
    if
	Name0 == Name ->
	    true;
	true ->
	    table_defined(Name0, Xs)
    end;
table_defined(Name, [_|Xs]) ->
    table_defined(Name, Xs);
table_defined(_Name, []) ->
    false.

%% prepend key-value pair IF value is not nil. This
%% is used to clean up output, basically.

cons(Key, [], Rest) ->
    Rest;
cons(Key, Val, Rest) ->
    [{Key, Val}|Rest].

