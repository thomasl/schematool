%%% File    : simple_tests.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  1 Apr 2014 by Thomas Lindgren <>

-module(simple_tests).
-export(
   [
   ]
  ).

%% Simple base schema
%% - one table
%% - no weird options, all nodes have same storage

base_schema(Tab, Type, Store, Nodes) ->
    [{table, Tab, 
      [{type, Type}, {Store, Nodes}]}].

%% Add node to table

add_node_test() ->
    [N0, N1] = exit(nyi),
    Sch0 = base_schema(add_node_table, set, disc_copies, [N0]),
    Sch1 = base_schema(add_node_table, set, disc_copies, [N0, N1]),
    Sch2 = [],
    migrate_chain([Sch0, Sch1, Sch2]).

%% Delete node from table

del_node_test() ->
    [N0, N1] = exit(nyi),
    Sch0 = base_schema(del_node_table, set, disc_copies, [N0, N1]),
    Sch1 = base_schema(del_node_table, set, disc_copies, [N0]),
    Sch2 = [],
    migrate_chain([Sch0, Sch1, Sch2]).

%% Variations, incl "migrate from A to B" where A cut B is empty.

%% Change table storage type
%% - migrate between all possible types

storage_tests() ->
    storage_test(disc_copies, ram_copies),
    storage_test(disc_copies, disc_only_copies),
    storage_test(ram_copies, disc_copies),
    storage_test(ram_copies, disc_only_copies),
    storage_test(disc_only_copies, ram_copies),
    storage_test(disc_only_copies, disc_copies).

%% Migrate from storage type A to B. Finally, clear the schema.
    
storage_test(From, To) ->
    [N0, N1] = exit(nyi),
    Sch0 = base_schema(st_table, set, From, [N0, N1]),
    Sch1 = base_schema(st_table, set, To, [N0]),
    Sch2 = [],
    migrate_chain([Sch0, Sch1, Sch2]).

%% Change table type (set, ordered_set, bag)

table_type_tests() ->
    table_type_test(ordered_set, set),
    table_type_test(ordered_set, bag),
    table_type_test(set, ordered_set),
    table_type_test(set, bag),
    table_type_test(bag, ordered_set),
    table_type_test(bag, set).
    
table_type_test(From, To) ->
    [N0, N1] = exit(nyi),
    Sch0 = base_schema(ty_table, From, ram_copies, [N0, N1]),
    Sch1 = base_schema(ty_table, To, ram_copies, [N0]),
    Sch2 = [],
    migrate_chain([Sch0, Sch1, Sch2]).

%% Combine type and storage type changes

%% 
%% Table transforms [1 record]
%% - add/del attribute
%% - copy attribute
%% - increment attribute
%% - compute attr (sum or something)
%% - stacked changes
%%
%% 1/ create table A
%% 2/ insert record recA
%% 3/ migrate to B, recA -> recB
%% 4/ verify that recB is correct
%% 5/ clear the schema
%%
%% NB: simplistic setup, 1 node, etc

%% As, Bs = record attribute key-value lists
%% RecAs, RecBs = lists of values to be written and read
%% Xfs = list of transforms

transform_table(As, Bs, RecAs, RecBs, Xfs) ->
    [N0] = exit(nyi),
    _KeyA = hd(RecAs),
    KeyB = hd(RecBs),
    RecA = list_to_tuple([tr_table|RecAs]),
    RecB = list_to_tuple([tr_table|RecBs]),
    Sch0 = layout_schema(tr_table, As, []),
    Sch1 = layout_schema(tr_table, Bs, Xfs),
    Sch2 = [],
    migrate_chain([Sch0]),
    mnesia:transaction(
      fun() -> mnesia:write(RecA) end
     ),
    migrate_chain([Sch1]),
    %% check record recB, esp. xform
    case mnesia:transaction(
	   fun() -> mnesia:read(tr_table, KeyB) end
	  ) of
	{atomic, ReadB} ->
	    %% should do a compare and show attribute, value, desired value
	    %% for those that do not match (+ handle recname diff)
	    io:format("Wanted RecB ~p\nGot ~p\n (perhaps ok)\n", [RecB, ReadB])
    end,
    %% Sch2 deletes the table
    migrate_chain([Sch2]).

%% A simple lib function to test simple transforms of single attributes
%% (This is probably most of the practical cases.)
%%
%% N is the node to use (should we pass Ns?)
%%
%% We create record specs for before and after layout transform and
%% write a sample record to the table after creating it.
%%
%% UNFINISHED
%% - rollback on failure? not done
%% - should use schema migration rather than invoking the
%%   internals

simple_transform_table(N, {attr, A, Seed, Xform, Expected}) ->
    Tab = layo,
    Attrs = [key, A, orig1, orig2],
    RecDef = list_to_tuple([Tab|Attrs]),
    Key = 0,
    Rec = {Tab, Key, Seed, 10, 42},
    Opts = [{type, set}, {ram_copies, [N]},
	    {attributes, Attrs},
	    {record, Rec}],
    Sch0 = [{nodes, [N]}, {table, Tab, Opts}],
    migrate_chain([Sch0]),
    mnesia:transaction(fun() -> mnesia:write(Rec) end),
    %% The old and new table have the same attributes (which is unusual),
    %% we just do the Xform
    Old = {Tab, Attrs},
    New = {Tab, Attrs},
    schematool_helper:transform_table_layout(Tab, [Xform], Old, New),
    case mnesia:transaction(fun() -> mnesia:read(Tab, Key) end) of
	{atomic, [OutRec]} ->
	    %% Check that result is okay
	    case OutRec of
		{Tab, _Key, NewA, _NewOrig1, _NewOrig2} ->
		    if
			NewA == Expected ->
			    ok;
			true ->
			    exit({attribute_value, NewA, {should_be, Expected}})
		    end;
		Other ->
		    exit({read_records_not_compliant, Other})
	    end;
	Other ->
	    exit({read_unsuccessful, Other})
    end.

%% Attrs are given as {AttrName, Default},
%% or just AttrName.
%%
%% We assume record name = table name.
%%
%% Presumably, nodes, table type and storage does not matter
%% since we're just testing record rewrites. So we just use
%% convenient values for these.
%%
%% Transforms:
%%   {attr, A, PrevA}
%%   {attr, A, fun(Old, KVs) -> Val}
%%   {rec, fun(KVs) -> NewKVs}

layout_schema(Tab, Attrs, Xforms) ->
    [N] = exit(nyi),
    RecName = Tab,
    {AttrNames, Rec} = rec_attrs(RecName, Attrs),
    [{table, Tab, 
      [{attributes, AttrNames},
       {record, Rec},
       {type, set},
       {ram_copies, [N]},
       {transform, "no-vsn", Xforms}]}].

%%

rec_attrs(Rec, Attrs) ->
    {AttrNames, AttrVals} = rec_attrs(Attrs, [], []),
    {AttrNames, list_to_tuple([Rec|AttrVals])}.

rec_attrs([], RevNames, RevVals) ->
    {lists:reverse(RevNames), lists:reverse(RevVals)};
rec_attrs([Attr|Attrs], RevNames, RevVals) when is_atom(Attr) ->
    Val = undefined,
    rec_attrs(Attrs, [Attr|RevNames], [Val|RevVals]).

%%

rec_diff(Attrs, RecB0, RecB1) ->
    [_|RecBs0] = tuple_to_list(RecB0),
    [_|RecBs1] = tuple_to_list(RecB1),
    rec_diff_attrs(Attrs, RecBs0, RecBs1, []).

%%

rec_diff_attrs([Attr|Attrs], [Val0|Vals0], [Val1|Vals1], RevDiffs0) ->
    RevDiffs1 =
	if
	    Val0 =/= Val1 ->
		[{Attr, Val0, Val1}|RevDiffs0];
	    true ->
		RevDiffs0
	end,
    rec_diff_attrs(Attrs, Vals0, Vals1, RevDiffs1);
rec_diff_attrs([], [], [], RevDiffs) ->
    lists:reverse(RevDiffs).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Minor mnesia options

%% Fragmented tables

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Install schematool, then do migrations
%%
%% - mnesia must be stopped prior to starting schematool
%% - make sure we can do dangerous schema operations (delete)
%%
%% UNFINISHED
%% - delete schema + uninstall schematool first (if needed)
%% - check return values

install(Nodes) ->
    mnesia:stop(),
    schematool_admin:install(Nodes),
    schematool_admin:allow_full_delete(true),
    ok.

%% Prepare for install by deleting existing stuff

prep_install() ->
    schematool_admin:allow_full_delete(true),
    schematool_admin:delete().
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Library function: given a sequence of
%% schemas, migrate between them.
%%
%% 1/ check there is no current schema (for _orig version)
%% 2/ load schemas, schematool_admin:load_shema_def(S)
%% 3/ migrate to each of the schemas in sequence
%%
%% NB: We may want to do up/down-migrations too, they are
%% handled in migrate_rollback/1 and migrate_rollback_chain/1.

migrate_chain_orig(Schemas) ->
    case schematool_admin:current_schema() of
	undefined ->
	    migrate_chain(Schemas);
	Other ->
	    %% If there already is a schema, stop
	    {error, {node_already_has_schema, Other}}
    end.

migrate_chain(Schemas) ->
    Schema_keys =
	[ schematool_admin:load_schema_def(S) || S <- Schemas ],
    migrate_keys(Schema_keys).

migrate_keys([Schema_key|Schema_keys]) ->
    io:format("Migrate ~p -> ~p\n", 
	      [schematool_admin:current_schema(), Schema_key]),
    schematool_admin:migrate_to(Schema_key),
    migrate_keys(Schema_keys);
migrate_keys([]) ->
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Migration and (re-migrated) rollback.
%%
%% Migrate to the given key, then go back again.
%%  If no schema installed, error.

migrate_rollback(To) ->
    case schematool_admin:current_schema() of
	undefined ->
	    {error, no_current_schema};
	From ->
	    To = schematool_admin:migrate_to(To),
	    From = schematool_admin:migrate_to(From)
    end.

migrate_rollback_chain([]) ->
    ok;
migrate_rollback_chain(Schemas) ->
    case schematool_admin:current_schema() of
	undefined ->
	    SchemaKeys = [ schematool_admin:load_schema_def(S) 
			   || S <- Schemas ],
	    %% given keys S0, S1, S2, we construct the chain
	    %%  S0, S1, S2, S1, S0
	    MigChain = SchemaKeys ++ tl(lists:reverse(SchemaKeys)),
	    migrate_keys(MigChain);
	CurrKey ->
	    {error, {node_already_has_schema, CurrKey}}
    end.
