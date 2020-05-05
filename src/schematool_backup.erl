%%% File    : schematool_backup.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 14 Mar 2014 by Thomas Lindgren <>

%% Schematool backup and rollback for migrations
%%
%% Basically:
%% 1. activate checkpoint
%% 2. take a backup
%% 3. perform schematool migration
%% 4. if successful, deactivate checkpoint
%% 5. if failed, install fallback
%%
%% Note: may be too expensive; we should be able to detect when
%% rollback can be done more cheaply (ie, when migration is "easily
%% reversible").
%%
%% (Also note that this way of rolling back seems to REQUIRE restart
%% of the nodes, which is icky.)

-module(schematool_backup).
-export(
   [
    migrate/2,
    migrate/4
   ]
  ).

%% This is a wrapper: extract the table names from the schema
%% and use those.
%%
%% Old_Key is the key of the ORIGINATING schema.
%% Opaque and Args are set to uninteresting values.
%%  The value of Opaque is a unique tempfile
%% Migrate is assumed to be a fun that runs the migration
%%  actions and returns ok if they all succeed.
%%
%% UNFINISHED
%% - for mnesia_backup behaviour:
%%   - write tempfile to config'd directory
%%   - remove tempfile when done [maybe]
%% - should write changelog records before and after migration

migrate(Old_Key, Migration) ->
    Opaque = schematool_time:opaque_backup_key(),
    Args = [],
    Tabs = schematool_admin:tables_of(Old_Key),
    migrate(Opaque, Args, Tabs, Migration).

%% Status: trivial migration seems to work, mostly
%% - successful trivial migration: ok
%% - failed trivial migration: ok
%% - only tried on trivial data and a single node yet
%%
%% Opaque = opaque arg passed around (see man pages)
%%   this is passed to the backup module, the default
%%   one (which we use) interprets it as a filename
%% Args = checkpoint args
%% Tabs = list of tables in schema + schematool tables
%% Migration() -> ok | Error
%%  This collects the actual migration actions as a fun.
%%  If the fun returns ok, migration successful. Otherwise,
%%  we need to rollback.
%%
%% If the migration fails, the fallback is installed and
%% the nodes are automatically restarted (if fallback is ok).
%%
%% Note: the Migration fun cannot be run in a txn, because
%%  some of the operations are themselves txns and mnesia
%%  can't handle nested transactions. Therefore, the actions
%%  inside have to wrap in txns.
%%
%% UNFINISHED
%% - we always use the builtin mnesia_backup, this should
%%   be adjustable somewhere
%%   e.g., pass {backup, Mod, ConfArg} instead of Opaque

migrate(Opaque, Args0, Tabs, Migration) ->
    Args = [{max, Tabs}|Args0],
    FallbackArgs = [],
    case mnesia:activate_checkpoint(Args) of
	{error, ActErr} ->
	    io:format("activate_checkpoint failed: ~p\n", [ActErr]);
	{ok, Name, Nodes} ->
	    io:format("checkpoint activated on ~w -> ~w\n", [Nodes, Name]),
	    case mnesia:backup_checkpoint(Name, Opaque) of
		{error, BackErr} ->
		    io:format("backup_checkpoint failed: ~p\n", [BackErr]);
		ok ->
		    case catch Migration() of
			ok ->
			    io:format("Migration successful\n", []),
			    mnesia:deactivate_checkpoint(Name);
			Err ->
			    io:format("Migration failed: ~p\nAttempt rollback\n",
				      [Err]),
			    case mnesia:install_fallback(Opaque, FallbackArgs) of
				ok ->
				    io:format("Fallback installed\n", []),
				    deactivate_checkpoint(Name),
				    restart_nodes(Nodes);
				{error, FallbackErr} ->
				    io:format("Install fallback failed: ~p\n",
					      [FallbackErr]),
				    deactivate_checkpoint(Name)
			    end
		    end
	    end
    end.

restart_nodes(Nodes) ->
    OtherNodes = Nodes -- [node()],
    lists:foreach(
      fun(N) ->
	      io:format("Restarting node ~p -> ~p\n",
			[N, rpc:call(N, init, restart, [])])
      end,
      OtherNodes),
    io:format("Restarting self (~p)\n", [node()]),
    init:restart().

%% Refactored since it reoccurs several times

deactivate_checkpoint(Name) ->
    case mnesia:deactivate_checkpoint(Name) of
	ok ->
	    ok;
	{error, DeactErr} ->
	    io:format("Deactivating checkpoint ~w failed: ~p\n",
		      [Name, DeactErr])
    end.
    
%% UNFINISHED
%% - do we need something stronger than a plain transaction?
%% - use mnesia:activity to guide?

atomic_txn(Fun) ->
    case mnesia:transaction(Fun) of
	{atomic, Res} ->
	    Res;
	Err ->
	    Err
    end.
