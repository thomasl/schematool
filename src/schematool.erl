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

%% STATUS
%% - still early days

-module(schematool).
-export(
   [module/1,
    create_schema/1,
    diff/2,
    diff_modules/2
   ]).

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

module([M]) when is_atom(M) ->
    %% (when used on command line)
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

cr_schema(Schema) ->
    application:load(mnesia),
    Nodes = get_nodes(Schema),
    if
	Nodes == [] ->
	    io:format("No nodes found, stopping\n",[]);
	true ->
	    io:format("- schema for nodes ~p\n", [Nodes]),
	    case lists:member(node(), Nodes) of
		true ->
		    case mnesia:create_schema(Nodes) of
			{error, Rsn} ->
			    io:format("Unable to create fresh schema ~p: ~p\n", [Nodes, Rsn]);
			ok ->
			    %% Now create the tables and do whatever other setup
			    %% is needed
			    application:start(mnesia),
			    create_schematool_info(Nodes, Schema),
			    lists:foreach(
			      fun({table, Tab, Opts}) ->
				      %% check return value
				      io:format("Create table ~p ~p-> ~p\n", 
						[Tab, Opts, mnesia:create_table(Tab, Opts)]);
				 (Other) ->
				      %% skip the others
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

%%

get_nodes(Schema) ->
    proplists:get_value(nodes, Schema, [node()]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Store the current and previous schema versions.
%%
%% The key is {{YYYY, MM, DD}, {H, M, S}}.
%%
%% UNFINISHED
%% - how do we UPGRADE/MIGRATE this hidden table?
%% - retire old schema versions
%% - use erlang:now() instead of datetime?
%%   + store datetime as readable string/binary field

-define(schematool_info, schematool_info).
-record(schematool_info, {datetime, schema}).

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

add_schema_info(Datetime, Schema) ->
    mnesia:write(#schematool_info{datetime=Datetime,
				  schema=Schema}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Diff schema1 and schema2 (old vs new). Foundation for
%% performing upgrade/downgrade/...
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
%% UNFINISHED
%% - not sure if ordering is good
%% - the actions need to be revised
%%   * create table with correct options

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
    [ {schematool_helper, create_table_all_nodes, [N1, Add]} 
      || {table, Add, Opts} <- Added ] ++
    [ {schematool_helper, delete_table_all_nodes, [N0, Del]} 
      || Del <- Deleted ] ++
    [ schematool_table:alter_table(Chg) || Chg <- Changed ].

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

