%%% File    : simple_tests.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  1 Apr 2014 by Thomas Lindgren <>

-module(simple_tests).
-export(
   [
   ]
  ).

%% Add node to table
%% Delete node from table
%% Variations, incl "migrate from A to B"

%% Change table storage type

%% Change table type

%% Table transforms [1 record]
%% - add/del attribute
%% - copy attribute
%% - increment attribute
%% - compute attr (sum or something)
%% - stacked changes
%%
%% + verify that changes are OK

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Install schematool, then do migrations

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Library function: given a sequence of
%% schemas, migrate between them.
%%
%% 1/ load schemas, schematool_admin:load_shema_def(S)
%% 2/ check that there is no current schema
%% 3/ migrate to each of the schemas in sequence
%%
%% NB: We may want to do up/down-migrations too, they are
%% handled in another function.

migrate_chain(Schemas) ->
    case schematool_admin:current_schema() of
	undefined ->
	    Schema_keys =
		[ schematool_admin:load_schema_def(S) || S <- Schemas ],
	    migrate_keys(Schema_keys);
	Other ->
	    %% If there already is a schema, stop
	    {error, {node_already_has_schema, Other}}
    end.

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
