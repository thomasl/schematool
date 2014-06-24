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
%%   (For examples, see Makefile for WF or erlmail.)
%%
%% This module is the start of schema processing once we
%% have the schema module compiled and generated.
%%
%% The schema is stored in a file foo.schema, which
%% is preprocessed into foo.schema.term by schematool.pl.
%% Note that multiple schema.term files can be merged
%% into a master schema, which is suitable for applications
%% defining their own local schemas.
%%
%% The foo.schema.term file can then be loaded into the
%% system and viewed among the candidate schemas. The candidates
%% are indexed by load time, and are stored persistently.
%%
%% Finally, the system can migrate to a candidate schema.
%% This takes a backup of the system tables, perform the
%% changes and stops with the new model. If migration fails,
%% the system rolls back to the backup and possibly restarts.
%% (This is entirely handled by the migration process).
%%
%% Internally, schematool adds a small number of internal
%% tables to hold information such as the candidate schemas,
%% a changelog and some internal values.
%%
%% Advanced topics:
%% - qlc
%% - automatic accessors (using indexes etc, in a
%%   single consistent notation) = query language

%% STATUS
%% - creates schema from spec routinely (on 1 node)
%% - plausible migration plans generated
%% - performed a 1-record layout transformation
%%   * black triangle day :-)
%%   * does NOT properly detect that migration failed
%% - schema merge supported
%%   * still some usage issues, e.g., 'all_nodes'
%%
%% UNFINISHED
%% - reuse parametrized schema
%%   * with renamed nodes (everywhere)
%%   * extended (= explicit diff)
%% - derive schema by checking mnesia
%% - verify schema (derived vs latest)
%% - repair schema: diff

-module(schematool).
-export(
   [
    install/1,
    load_and_migrate/1,
    load_schema/1,
    migrate_to/1,
    diff/2,
    diff_keys/2,
    merge/1,
    merge/2
   ]).

-export([view_schemas/0,
	 schema_keys/0,
	 get_schema/1,
	 delete_schema/1,
	 current_schema/0
	]).

-include("schematool.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

install(Nodes) ->
    schematool_admin:install(Nodes),
    %% printout (is this useful?)
    mnesia:info().

load_and_migrate(File) ->
    Schema_key = schematool_admin:load_schema_file(File),
    schematool_admin:migrate_to(Schema_key).

load_schema(File) ->
    schematool_admin:load_schema_file(File).

migrate_to(Schema_key) ->
    schematool_admin:migrate_to(Schema_key).

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

history() ->
    schematool_admin:view_history().

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Merge multiple schemas into one
%%
%% - read all the schemas
%% - check for conflicts
%% - merge schemas if ok + add annotation
%%
%% Note: uses of 'all_nodes' are everywhere replaced by the list of
%% nodes given (perhaps empty).

merge(Files) ->
    io:format("Warning: merge/1: no nodes given, using empty list", []),
    merge(Files, []).

merge(Files, Nodes) ->
    Schemas = [ {F, schematool_admin:consult(F)} || F <- Files ],
    PreSchema = merge_schemas(Schemas),
    subst(all_nodes, Nodes, PreSchema).

%% merge_with: merge a schema with a processed other schema.
%%
%% (We process the initial schema to make conflict checking cheaper)
%%
%% UNFINISHED
%% - Others, like vsn and nodes, need to be handled for this to work
%%   * 'vsn' applies only to the schema file
%%   * 'nodes' applies only to schema file, but should perhaps be added
%%     from the outside
%%   * note that table nodes may be a bit iffy too! we should perhaps
%%     have a placeholder

merge_schemas(Schemas) ->
    to_list(
      lists:foldl(
	fun merge_schema/2,
	empty_schema(),
	Schemas)).

merge_schema({File, Schema}, PrevSch0) ->
    lists:foldl(
      fun({table, Tab, Opts0}, PrevSch) ->
	      Opts = canonical_table_opts(Opts0),
	      case lookup(Tab, PrevSch) of
		  not_found ->
		      %% OK, update
		      insert(Tab, Opts, File, PrevSch);
		  {found, PrevOpts, PrevFiles} ->
		      if
			  Opts == PrevOpts ->
			      %% mark Tab as also defined in File
			      io:format("Warning: table ~p defined in multiple schemas\n"
					" ~p\n",
					[Tab, PrevFiles ++ [File]]),
			      also_in_file(Tab, File, PrevSch);
			  true ->
			      %% clash, log error
			      io:format("Error: Schema clash: table ~p declared with\n"
					" prev opts: ~p\n curr opts: ~p\n"
					" curr file ~p\n"
					" prev files: ~p\n",
					[Tab, PrevOpts, Opts,
					 File, PrevFiles]),
			      exit(unable_to_merge_schemas)
		      end
	      end;
	 (Other, PrevSch) ->
	      append_to(PrevSch, Other)
      end,
      PrevSch0,
      Schema).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% the ADT

empty_schema() ->
    {dict:new(), []}.

lookup(Tab, {TabDefs, _Others}) ->
    case dict:find(Tab, TabDefs) of
	{ok, {Value, Files}} ->
	    {found, Value, Files};
	_ ->
	    not_found
    end.

insert(Tab, Opts, File, {TabDefs, Others}) ->
    {dict:store(Tab, {Opts, [File]}, TabDefs), Others}.

%% We drop the files of origin, is this OK?

to_list({TabDefs, Others}) ->
    Others ++ [ {table, Tab, Opts} || {Tab, {Opts, _Files}} <- dict:to_list(TabDefs) ].

%% Update, add File to the Files of the Tab (which should exist)

also_in_file(Tab, File, {TabDefs, Others}) ->
    NewTabDefs = dict:update(
		   Tab, 
		   fun({Opts, Files}) ->
			   {Opts, Files ++ [File]}
		   end,
		   [File],
		   TabDefs),
    {NewTabDefs, Others}.

%% NB: could store Others reversed and convert appropriately
%% in to_list/1, but there are usually ~1 Other per schema so
%% do it when there's need.

append_to({TabDefs, Others}, Other) ->
    {TabDefs, Others ++ [Other]}.

%% To check for option sameness, we sort the options and
%% then check structural equality (==). This is insufficient
%% if there are, say, suboptions that should be canonicalized too,
%% or options where the default is same. (In short, further
%% canonicalization is needed to do it just right.)
%%
canonical_table_opts(Opts) ->
    lists:sort(Opts).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Diff the two schemas, returning instructions to go from old S0 to
%% new S1.
%%
%% Compute the tables added, deleted or changed between the schemas.
%% Added tables are created, deleted tables are deleted.  For
%% (potentially) changed tables, we use schematool2:alter_table to find
%% instructions to migrate from S0 to S1.
%%
%% NEW VERSION
%% - only diff tables, skip {nodes, Ns} for now (or anything else)

diff_keys(K0, K1) ->
    diff(schematool_admin:def(K0), schematool_admin:def(K1)).

diff(S0, S1) ->
    {Added, Changed, Deleted} = table_changes(S0, S1),
    lists:flatten(
      [ [ {mnesia, create_table, [Tab, Opts]} || {table, Tab, Opts} <- Added ],
	[ {mnesia, delete_table, [Tab]} || {table, Tab, _Opts} <- Deleted ],
	[ schematool2:alter_table(Tab, Old_opts, New_opts)
	  || {table, Tab, Old_opts, New_opts} <- Changed ]
       ]).

%% Tables in S1 but not in S0 are added.
%% Tables in S0 but not in S1 are deleted.
%% Tables in both S0 and S1 are (potentially) changed.

table_changes(S0, S1) ->
    D0 = dict:from_list([ {Tab, {Opts, undefined}} || {table, Tab, Opts} <- S0 ]),
    D1 = dict:from_list([ {Tab, {undefined, Opts}} || {table, Tab, Opts} <- S1 ]),
    D2 = dict:merge(
	   fun(Tab, {Old_opts, undefined}, {undefined, New_opts}) ->
		   {Old_opts, New_opts}
	   end,
	   D0,
	   D1),
    changes(dict:to_list(D2)).

%% Construct the lists of A/D/C tables

changes(Lst) ->
    RevA = [],
    RevD = [],
    RevC = [],
    changes(Lst, RevA, RevD, RevC).

changes([{Tab, {undefined, Opts}}|Xs], RevA, RevD, RevC) ->
    changes(Xs, [{table, Tab, Opts}|RevA], RevD, RevC);
changes([{Tab, {Opts, undefined}}|Xs], RevA, RevD, RevC) ->
    changes(Xs, RevA, [{table, Tab, Opts}|RevD], RevC);
changes([{Tab, {Old_opts, New_opts}}|Xs], RevA, RevD, RevC) ->
    changes(Xs, RevA, RevD, [{table, Tab, Old_opts, New_opts}|RevC]);
changes([], RevA, RevD, RevC) ->
    {lists:reverse(RevA), lists:reverse(RevD), lists:reverse(RevC)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% OBSOLETE?
%%
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
%%
%% The diff is a list of
%%  {nodes, Change}
%%  {tables, Change}
%%
%% NB: not sure if the nodes diff is interesting anymore?

%% OBSOLETE?

diff_old(S0, S1) ->
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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Used primarily to replace 'well-known keys' like 'all_nodes' in
%% schema. This makes the schemas more portable.

subst(Key, Val, [X|Xs]) ->
    [subst(Key, Val, X)|subst(Key, Val, Xs)];
subst(Key, Val, []) ->
    [];
subst(Key, Val, Tuple) when is_tuple(Tuple) ->
    list_to_tuple([ subst(Key, Val, X) || X <- tuple_to_list(Tuple) ]);
subst(Key, Val, Term) when is_atom(Term) ->
    if
	Key == Term ->
	    Val;
	true ->
	    Term
    end;
subst(_Key, _Val, Term) ->
    Term.

    
