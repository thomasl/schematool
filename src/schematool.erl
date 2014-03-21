%%% File    : schematool.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 22 Jan 2014 by Thomas Lindgren <>

%% Schematool status
%% - initialize schema db
%% - generate db.schema -> db.schema.term via a
%%   toolchain (because epp is a joke at the
%%   expense of mainkind)
%% - load db.schema.term into db
%% - migrate between schemas using
%%   * schematool generates migration actions
%%       UNFINISHED - these need to be marshalled
%%   * schematool_backup acts as a wrapper,
%%     including rollback on failure (distributed!)
%% - UNFINISHED
%%   * previous attempts at schema management should
%%     be removed, see below
%%
%% However, note that migration can be time consuming
%% in some cases, e.g., that a lot of data need to be
%% rewritten and/or copied. 
%%
%% NOTE: to run schematool, see ../bin/schematool.pl
%%   (Also see Makefile for WF or erlmail.)
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
%%   before any dangerous/lossy migration
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
%% - loaded schemas with existing vsn => warning
%%   use {vsn, "foo"} attribute
%%   * vsn = undefined skipped
%%   * lookup by vsn
%% - reuse schema
%%   * with renamed nodes (everywhere)
%%   * extended (= explicit diff)
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

-module(schematool2).
-export(
   [
    create_schema/2,
    load_schema/1,
    diff/2
   ]).

-export([view_schemas/0,
	 schema_keys/0,
	 get_schema/1,
	 delete_schema/1,
	 current_schema/0
	]).

-export(
   [tables_of/1,
    schematool_tables/0,
    schematool_table_type/0
   ]).

-include("schematool.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

install(Nodes) ->
    schematool_admin:install(Nodes).

%% OBSOLETE
%% - (load + migrate) as an action

create_schema(Nodes, File) ->
    install(Nodes),
    Schema_key = load_schema(File),
    schematool_admin:migrate_to(Schema_key).

load_schema(File) ->
    schematool_admin:load_schema_file(File).

view_schemas() ->
    [ schematool_admin:definition(Key) || Key <- schema_keys() ].

schema_keys() ->
    schematool_admin:candidate_schemas().

get_schema(Key) ->
    schematool_admin:definition(Key).

delete_schema(Key) ->
    schematool_admin:delete_schema(Key).

current_schema() ->
    schematool_admin:current_schema().

tables_of(Schema_key) ->
    schematool_admin:tables_of(Schema_key).

schematool_tables() ->
    exit(nyi).

schematool_table_type() ->
    exit(nyi).

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

%% 

get_nodes(Schema) ->
    [ node_name_of(N) || N <- proplists:get_value(nodes, Schema, [node()]) ].

%% OBSOLETE
%% - only Node = atom() permitted for now

node_name_of({Node, _Startup}) ->
    Node;
node_name_of(Node) when is_atom(Node) ->
    Node.

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

