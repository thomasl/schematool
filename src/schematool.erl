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

%% STATUS
%% - early

-module(schematool).
-export(
   [module/1,
    create_schema/1
   ]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-type schema() :: [schema_item()].   %% The main type of interest

-type schema_item() :: 
     {nodes, [node_name()]}
   | {table, table_name(), [table_opt()]}.

-type node_name() :: atom().   %% is there a node name type?
-type table_name() :: atom().
-type table_opt() :: term().   %% could be more precise

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
%% mnesia before creating the tables though.
%% 
%% If the current node is not part of the schema, then skip creating
%% schema or tables. This is helpful when we just want to run the same
%% scripts for all nodes regardless of whether they run mnesia or not.
%%
%% UNFINISHED
%% - no error checking, e.g., db already exists
%% - maybe application:stop(mnesia) afterwards?

cr_schema(Schema) ->
    application:load(mnesia),
    Nodes = get_nodes(Schema),
    case lists:member(node(), Nodes) of
	true ->
	    case mnesia:create_schema(Nodes) of
		{error, Rsn} ->
		    io:format("Unable to create fresh schema ~p: ~p\n", [Nodes, Rsn]);
		ok ->
		    %% Now create the tables and do whatever other setup
		    %% is needed
		    application:start(mnesia),
		    lists:foreach(
		      fun({table, Tab, Opts}) ->
			      %% check return value
			      mnesia:create_table(Tab, Opts);
			 (Other) ->
			      %% skip the others
			      ok
		      end,
		      Schema),
		    %% mnesia:info(), %% show status
		    %% application:stop(mnesia),
		    ok
	    end;
	false ->
	    %% Current node is NOT part of schema,
	    %% do not create tables
	    %% - warning or log this? verbose option, could
	    %%   be annoying to run in scripts otherwise
	    ok
    end.

%%

get_nodes(Schema) ->
    get_value(nodes, Schema, [node()]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Diff schema1 and schema2 (old vs new). Foundation for
%% performing upgrade/downgrade/...
%% 
%% Returns the following [currently!]:
%% - nodes added/deleted in schema2
%% - tables added/deleted/changed (new and old) in schema2
%% 
%% Should also allow
%% - RENAMING of tables in some reasonable way
%% - (nice feature: rename/manipulate record fields)
%% - accessors? (not implemented yet)
%%
%% NOTE: This MUST be in sync with the schema spec format.
%% 
%% NB: The algorithms used are very basic, one of them is
%%  O(N*M). Since N and M are assumed to be small, this is
%%  okay. If not, rewrite to get better ordo (e.g., store
%%  tables in dict/hash, etc.)
%%
%% NB: current format just returns UNCHANGED keys, which is good
%% for testing.

diff(S0, S1) ->
    N = nodediff(S0, S1),
    Ts = tablediff(S0, S1),
    cons(nodes, N, cons(tables, Ts, [])).

%% (assume nodes do not have repeated elements, maybe we should
%% use sets or uniquify?)

nodediff(S0, S1) ->
    N0 = get_nodes(S0),
    N1 = get_nodes(S1),
    Added = N1 -- N0,
    Deleted = N0 -- N1,
    cons(added, Added, cons(deleted, Deleted, [])).

%% Collect tables that have been added, deleted or changed.

tablediff(S0, S1) ->
    %% Select tables from schema def, since it's a list
    %% at the moment, we just keep them
    T0 = S0,
    T1 = S1,
    Added = [ Table || Table <- S1,
		       not table_defined(table_name(Table), S0) ],
    Deleted = [ Table || Table <- S0,
		       not table_defined(table_name(Table), S1) ],
    Changed = tables_changed(T0, T1),
    cons(added, Added, cons(deleted, Deleted, cons(changed, Changed, []))).

%% Lookup the tables found in both lists of tables and check
%% if their options field differ. If so, return both. [Added
%% or deleted tables are handled elsewhere.]
%%
%% Non-table definitions are skipped.

tables_changed([{table, Name, OldOpts}|Ts], Tabs) ->
    case table_def(Name, Tabs) of
	not_found ->
	    tables_changed(Ts, Tabs);
	{found, {table, _Name, NewOpts}} ->
	    if
		OldOpts == NewOpts ->
		    tables_changed(Ts, Tabs);
		true ->
		    [{table_diff, Name, OldOpts, NewOpts}|tables_changed(Ts, Tabs)]
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
table_def(_Name, []) ->
    not_found.

%% Is table Name0 defined in schema?
		
table_defined(Name0, [{table, Name, _Opts}|Xs]) ->
    if
	Name0 == Name ->
	    true;
	true ->
	    table_defined(Name0, Xs)
    end;
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
%% UNFINISHED
%% - (Should Dflt be a closure, evaluated only if not found?
%%   If so, return Dflt() instead of Dflt.)

get_value(Key, [{Key, Val}|Xs], Dflt) ->
    Val;
get_value(Key, [_|Xs], Dflt) ->
    get_value(Key, Xs, Dflt);
get_value(Key, [], Dflt) ->
    Dflt.

