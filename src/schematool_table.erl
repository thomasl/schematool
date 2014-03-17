%%% File    : schematool_table.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 29 Jan 2014 by Thomas Lindgren <>

%% Altering a table definition. This is done
%% given the old and new definitions.
%%
%% REWRITE:
%% - check if table needs to be copied
%%   * in this case, we can just create the new table
%%     the way it should end, then copy+xform the data
%%     (twice...)
%%   * if not, layout changes + update table options
%% - fragmented tables
%%   * detect that frags are used
%%   * change of #frags
%%   * migrate frag (per above)
%%   * frag <-> nonfrag
%% - derive a schema from current db
%% - verify that current db complies with schema 
%%
%% NOTE: decision whether copying is needed is VERY CENTRAL
%%  to how we proceed. Fragmented tables should also take
%%  this into account. [I don't _think_ fragmentation forces
%%  table copying though. In fact, it might be simplified.]
%%
%% NOTE: table copying might also be managed by backup
%%  and traverse_backup. This might be an implementation
%%  detail.
%% 
%% NOTE: looks like fragmented tables are explicitly
%%  updated to frag status, rather than declaring them.
%%  Schematool should provide a declarative approach
%%  rather than a procedural one.
%%
%% NOTE: should warn the user that migration will
%%  entail copying
%%
%% - might also want to tell that N records need to be
%%   migrated? table_info(Tab, size)
%%
%% UNFINISHED
%% 1/ collect ALL of the changes in one call
%%    (rec name, attrs, etc)
%% 2/ handle fragmented tables
%% 3/ check what happens with multiple storage types
%%    for a table (create schema & migrate)
%% 3/ when this looks okay, I think we can start debugging
%%    (and maybe running some migrations)

-module(schematool_table).
-export(
   [alter_table/1,
    alter_storage_type/4,
    copy_table/2,
    lossy_copy_table/3,
    migration_needs_table_copy/1
   ]).

-import(proplists, 
	[get_value/3,
	 get_all_values/2
	]).

%-define(dbg(Str, Xs), io:format(Str, Xs)).
-define(dbg(Str, Xs), ok).

%% Alter table so that it obeys New_opts
%% starting from Old_opts.
%%
%% Note that some properties can't be changed
%% at the moment. For example, type and local
%% storage.
%%
%% What has to be done?
%% - lookup current options + DEFAULTS
%% - for each option, decide on action to
%%   do the change
%%   * some options can't be changed
%% - perform change options in appropriate order
%%
%% Note:
%% - if table size = 0, we can skip some actions
%%   * e.g., copying not needed
%% - if table size is small, we can do different
%%   strategy than if table is large
%%   * small table can migrate via ram_copies
%%
%% UNFINISHED
%% - what is the correct order of actions?
%%   e.g., change table opts before or after layout, etc
%%   * correctness
%%   * optimization (change layout in ram before
%%     going to disc...? or other way around)
%%   * should we specify dependences and sort?

alter_table({Tab, Old_opts, New_opts}=TabDiff) ->
    Size = num_records(Tab),
    Comment = {comment, 
	       lists:flatten(io_lib:format("~p has ~p records\n",
					   [Tab, Size]))},
    Acts0 = alter_table_options(TabDiff),
    Acts1 = alter_table_layout(Tab, Old_opts, New_opts, []),
    Acts2 = alter_storage_type(Tab, Old_opts, New_opts, []),
    Acts3 = alter_indexes(Tab, Old_opts, New_opts),
    lists:flatten(
      [Comment, Acts0, Acts1, Acts2, Acts3]
     ).

%% Compute number of records in table
%%
%% UNFINISHED
%% - ensure mnesia started, else we can falsely get size=0
%% - fragmented table size? check if this needs attention

num_records(Tab) ->
    mnesia:table_info(Tab, size).

%% Predicate: test if migration will require copying the table. This
%% test affects the migration strategy chosen by schematool.
%%
%% This may be needed because of table type or table options that
%% can't be adjusted after the table has been created.
%%
%% Note: this function is normally called when Old_opts and New_opts
%% differ in some respect.
%%
%% Note: does not consider fragmented tables.

migration_needs_table_copy({_Tab, Old_opts, New_opts}) ->
    lists:any(
      fun({type, New}) ->
	      option_not_same(New, type, set, Old_opts);
	 ({local_content, New}) ->
	      option_not_same(New, local_content, false, Old_opts);
	 ({storage_properties, New}) ->
	      option_not_same(New, storage_properties, undefined, Old_opts);
	 ({snmp, New}) ->
	      option_not_same(New, snmp, undefined, Old_opts);
	 (_Other) ->
	      false
      end,
      New_opts).

option_not_same(New, Opt, Dflt, Old_opts) ->
    Old = get_value(Opt, Old_opts, Dflt),
    if
	Old =/= New ->
	    true;
	true ->
	    false
    end.

%% Returns a list of instructions
%% - perhaps with a priority so we can sort them
%%   into "good order"?

alter_table_options({Tab, Old_opts, New_opts}) ->
    lists:foldr(
      fun({access_mode, Mode}, Actions) ->
	      Old = get_value(access_mode, Old_opts, read_write),
	      if
		  Mode =/= Old ->
		      [{mnesia, change_table_access_mode, [Tab, Mode]}|Actions];
		  true ->
		      Actions
	      end;
	 ({load_order, L}, Actions) ->
	      Old = get_value(load_order, Old_opts, 0),
	      if
		  L =/= Old ->
		      [{mnesia, change_table_load_order, [Tab, L]}|Actions];
		  true ->
		      Actions
	      end;
	 ({majority, M}, Actions) ->
	      %% check if changed from Old_opts, if so:
	      %%
	      %% UNFINISHED
	      %% - default value not documented in mnesia ref man
	      Dflt = true,
	      Old = get_value(majority, Old_opts, Dflt),
	      if
		  M =/= Old -> 
		      [{mnesia, change_table_majority, [Tab, M]}|Actions];
		  true ->
		      Actions
	      end;
	 ({disc_copies, Ns}, Actions) ->
	      %% Handled by alter_storage_type
	      Actions;
	 ({ram_copies, Ns}, Actions) ->
	      %% Handled by alter_storage_type
	      Actions;
	 ({disc_only_copies, Ns}, Actions) ->
	      %% Handled by alter_storage_type
	      Actions;
	 ({attributes, Attrs}, Actions) ->
	      %% Handled by alter_storage_type 
	      Actions;
	 ({record_name, Rec}, Actions) ->
	      %% Handled by alter_storage_type 
	      Actions;
	 ({index, Ixs}, Actions) ->
	      %% Handled by alter_indexes
	      Actions;
	 ({type, Type}=Opt, Actions) ->
	      %% check if changed
	      %%
	      %% UNFINISHED
	      %% - may be able to change this by creating new table B,
	      %%   copy A -> B, delete A, create A, copy B -> A
	      %%   [if we can rename the table, better ...]
	      Old = get_value(type, Old_opts, set),
	      if
		  Type =/= Old -> 
		      [{error, {unable_to_change, Tab, Opt, {previous, Old}}}|Actions];
		  true ->
		      Actions
	      end;
	 ({local_content, Bool}=Opt, Actions) ->
	      %% NB: no apparent way to change this property
	      Old = get_value(local_content, Old_opts, false),
	      if
		  Bool =/= Old -> 
		      [{error, {unable_to_change, Tab, Opt, {previous, Old}}}|Actions];
		  true ->
		      Actions
	      end;
	 ({snmp, _SNMP}=Opt, Actions) ->
	      %% UNFINISHED
	      %% - not sure what needs to be done or how
	      %%   to change at runtime
	      [{error, {not_implemented, Tab, Opt}}|Actions];
	 ({storage_properties, _St}=Opt, Actions) ->
	      %% UNFINISHED
	      %% - set properties of ets/dets, not sure
	      %%   if we need to do a full table copy to handle
	      %%   such changes? (investigate ets/dets APIs)
	      [{error, {not_implemented, Tab, Opt}}|Actions];
	 ({record, R}, Actions) ->
	      Actions;
	 (Opt, Actions) ->
	      [{error, {unknown_option, Tab, Opt}}|Actions]
      end,
      [],
      New_opts
     ).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%
%% Altering table type:
%%
%%   set -> bag: copy, at worst
%%   ordered_set -> bag: copy, at worst
%%   set -> ordered_set: copy
%%   ordered_set -> set: copy, at worst
%%   bag -> *: copy, with possible data loss
%%
%% Thus, when moving from bag to some other type, we
%% run the risk of data loss. Otherwise, copying is safe.
%%
%% Note that mnesia appears not to support changing the type of a
%% table at runtime.  We could generically do this (A -> B):
%%
%% 1/ create A', identical to A
%% 2/ copy data of A to A'
%% 3/ delete A
%% 4/ recreate A with new options
%% 5/ copy data of A' to A
%% 6/ delete A'
%%
%% If mnesia had a way to rename a table, we could instead do the
%% cheaper option (saving one copy!):
%%
%% 1'/ create new table A'
%% 2'/ copy data from A to A'
%% 3'/ delete A
%% 4'/ rename A' to A
%%
%% Note: if A is a bag, multiple values per key will lead to data loss
%% when copied to bag.  We could detect data loss before this point.
%% 
%% Note that the general case can change table type, storage type
%% and what nodes it can occur on. We should try to be intelligent
%% in these cases, e.g., stay out of disc_only_copies as long as
%% possible. [The even more general case can also change other
%% table options, but let's leave that for elsewhere.]
%%
%% Finally, generic copying also supports cases such as changing
%% other table options apart from type (e.g., storage options,
%% local storage, and whatnot), where copying may be required
%% to properly transition to the new table definition.
%%  Basically, copying the table to one with the new options seems
%% to solve MOST issues with table changes (not data loss though, nor
%% layout; not sure about distribution).
%%
%% UNFINISHED
%% - changes storage type at same time!
%%   * could also do layout xform in second copy, I think
%% - check that it works properly on distributed system
%% - check that it works for fragments ...
%% - storage type of TEMP COPY should be settable
%%   * ram_copies should be very fast (but needs more memory)
%%   * storing table on "better set of nodes" than old_opts MIGHT be a win
%%   * after copying, change table copies to those of New_opts
%%     (= replicate to new nodes) then do the final copying
%% - is there some easier way to do this? existing mnesia operation?
%% - idea: optimize for memory: explicit copying from node A to B?
%%   that way, we do not need to build a temp table (probably)

alter_table_type(Tab, Old_opts, New_opts, Old_type, New_type, Actions) ->
    TmpTab = temp_table_name(Tab),
    RecName = record_name_of(Tab, Old_opts),
    Tmp_opts = adjust_options(RecName, Old_opts),
    [{mnesia, create_table, [TmpTab, Tmp_opts]},
     {schematool_helper, copy_table, [Tab, TmpTab]},
     {mnesia, delete_table, [Tab]},
     {mnesia, create_table, [Tab, New_opts]},
     case {Old_type, New_type} of
	 {set, _} ->
	     {schematool_helper, copy_table, [TmpTab, Tab]};
	 {ordered_set, _} ->
	     {schematool_helper, copy_table, [TmpTab, Tab]};
	 {bag, _} ->
	     %% UNFINISHED
	     %% - get collision policy Coll from New_opts if possible
	     %%   'keep_some_value' is default 
	     %%   (= just copy and overwrite)
	     %%   - if target is 'bag' too, not lossy
	     Coll = keep_some_value,
	     {schematool_helper, lossy_copy_table, [Coll, TmpTab, Tab]}
     end,
     %% Finally delete the temporary table
     {mnesia, delete_table, [TmpTab]}
     |Actions].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Adjusting the nodes of the temporary table:
%% - the end action is to copy to the nodes in New_opts
%% - thus, no inherent need to have temp table on all
%%   old nodes
%% - however, MIGHT be better to copy slow tables to ram_copies
%%   on origin node before transferring?
%%   * if old table is disc_copies_only, could get slow
%%   * but using ram_copies could eat too much memory
%% - if table has multiple storage types, might be better
%%   to just choose to copy from the "fast" types?
%%
%% Clearly, some optimization thinking should be used
%% here to minimize the cost. Creating the temp table
%% on the old nodes should be safe, however.
%%
%% Other adjustments:
%% - adjust layout while copying? (combine with alter_table_layout)
%% - lossy copy can be done first, ie inherit type from New_opts
%%   (since this reducs the amount of data)

%% Opts are the options of the old table, adjust them to suit the
%% temporary table.
%%
%% 1/ set record_name to that used by the old table
%% 2/ [adjust storage type and nodes to optimize copying]
%%
%% UNFINISHED
%% - adjust storage type and nodes used etc

adjust_options(Tab, Opts) ->
    record_name_of(Tab, Opts).

record_name_of(Tab, [{record_name, _Rec}|Opts]=Lst) ->
    %% we have an explicit record name, use that
    Lst;
record_name_of(Tab, [Opt|Opts]) ->
    %% skip
    [Opt|record_name_of(Tab, Opts)];
record_name_of(Tab, []) ->
    %% In this case, there is no explicit record_name def
    %% so insert one
    [{record_name, Tab}].
	
%% essentially a gensym
%%
%% - to be completely safe, should first check whether TmpName already
%%   is an atom (list_to_existing_atom/1 fails, or something)

temp_table_name(Tab) ->
    {A, B, C} = erlang:now(),
    TmpName = 
	lists:flatten(
	  io_lib:format("~p_~p_~p_~p",
			[Tab, A, B, C])),
    list_to_atom(TmpName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Alter table layout, using schematool_transform
%%
%% - where are the attribute and record xforms?
%%   + how do we invoke them?
%%
%% UNFINISHED
%% - transforms are specified simplistically, we should have
%%   some sort of (Vsn -> Vsn) key like appup to avoid
%%   implicit dependences on "previous" schema
%% - we currently permit
%%     {transform, ..., Xf}
%%   where Xf is a schematool_transform table xform.
%% - should pass something that can select correctly versioned xforms

alter_table_layout(Tab, Old_opts, New_opts, Actions) ->
    Old_rec_def = schematool_transform:rec_def_of({table, Tab, Old_opts}),
    New_rec_def = schematool_transform:rec_def_of({table, Tab, New_opts}),
    Xforms = [ last_element(Xf) || Xf <- keysearch_all(transform, 1, New_opts) ],
    transform_table_layout(Tab, Old_rec_def, New_rec_def, Xforms, Actions).

transform_table_layout(Tab, Old_rec_def, New_rec_def, Xforms, Actions) ->
    if
	Old_rec_def == New_rec_def ->
	    if
		Xforms == [] ->
		    Actions;
		true ->
		    %% layout unchanged but transforms still specified
		    %% - should we warn+skip?
		    [{warning, {layout_unchanged_but_transforms, Tab, Xforms}},
		     {schematool_helper, transform_table_layout,
		      [Tab, Xforms, Old_rec_def, Old_rec_def]}|Actions]
	    end;
	true ->
	    %% attributes have changed, shuffle the table
	    if
		Xforms == [] ->
		    [{schematool_helper, transform_table_layout,
		      [Tab, Old_rec_def, New_rec_def]}|Actions];
		true ->
		    [{schematool_helper, transform_table_layout,
		      [Tab, Xforms, Old_rec_def, New_rec_def]}|Actions]
	    end
    end.

%% Lookup all occurrences with element N equalling Key,
%% returning a list
%%
%% - this allows us to collect all of
%%   {transform, Xf} and {transform, FromTo, Xf} and so on
%%   which is good for flexibility

keysearch_all(Key, N, [Item|Xs]) when is_tuple(Item) ->
    case catch element(N, Item) == Key of
	true ->
	    [Item|keysearch_all(Key, N, Xs)];
	_ ->
	    keysearch_all(Key, N, Xs)
    end;
keysearch_all(Key, N, [_|Xs]) ->
    keysearch_all(Key, N, Xs);
keysearch_all(_Key, _N, []) ->
    [].

%% Utility

last_element(T) when is_tuple(T) ->
    element(size(T), T).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% A table may have multiple storage types spread over several
%% nodes. We perform the changes on a node-basis:
%% - node may have table added or deleted
%% - node may have storage type changed
%%
%% UNFINISHED
%% - default type ... or is this a required option?
%% - currently returns [{Node, {Old_storage, New_storage}}]
%%   where Storage = undefined or one of the possible
%%   mnesia types
%%   * not sure what 'undefined' means here? e.g., is it VALID
%%     to get this?

alter_storage_type(Tab, Old_opts, New_opts, Actions) ->
    D0 = dict:new(),
    Old_dict0 = storage_types([ram_copies, disc_copies, disc_only_copies], Old_opts, D0),
    D1 = dict:new(),
    New_dict0 = storage_types([ram_copies, disc_copies, disc_only_copies], New_opts, D1),
    %% Merge the dictionaries
    %% - we do the map/2 to distinguish old from new nodes
    %%   even in the case where old and new do not overlap
    %%   (though this may be an error case!)
    Old_dict1 = dict:map(fun(Node, St) ->
				 {St, undefined}
			 end,
			 Old_dict0),
    New_dict1 = dict:map(fun(Node, St) ->
				 {undefined, St}
			 end,
			 New_dict0),
    Merge_dict =
	dict:merge(fun(Node, {Old, _}, {_, New}) ->
			   {Old, New}
		   end,
		   Old_dict1,
		   New_dict1),
    ?dbg("Merged dict: ~p\n", [dict:to_list(Merge_dict)]),
    lists:flatten(
      [[ alter_storage(Tab, KV) || KV <- dict:to_list(Merge_dict) ],
       Actions]).

storage_types(Types, Opts, Dict0) ->
    lists:foldl(
      fun(Type, Dict) ->
	      storage_type(Type, Opts, Dict)
      end,
      Dict0,
      Types).

storage_type(Type, Opts, Dict) ->
    lists:foldl(
      fun(Nodes, Dict_0) ->
	      lists:foldl(
		fun(Node, Dict_00) ->
			dict:store(Node, Type, Dict_00)
		end,
		Dict_0,
		Nodes
	       )
      end,
      Dict,
      get_all_values(Type, Opts)
     ).

%% Instructions to alter storage type of table.
%% - first case = no change, hopefully common
%% - second and third: not sure, may be error cases
%%   * we do add/del_table_copy for now
%%   * basically, case 2 = no type in prev schema
%%     case 3 = no type in next schema
%% - fourth is actual change
%%
%% UNFINISHED
%% - are cases 2/3 errors instead? investigate
%% - should ping the Node before add/del
%%   (to get better error messages)

alter_storage(_Tab, {_Node, {Type, Type}}) ->
    %% No change
    [];
alter_storage(Tab, {Node, {undefined, Type}}) ->
    {mnesia, add_table_copy, [Tab, Node, Type]};
alter_storage(Tab, {Node, {Type, undefined}}) ->
    {mnesia, del_table_copy, [Tab, Node]};
alter_storage(Tab, {Node, {Type0, Type1}}) when Type0 =/= Type1 ->
    {mnesia, change_table_copy_type, [Tab, Node, Type1]}.

%% Generate actions to add and delete indexes by looking
%% at what indexes are defined in old and new opts.

alter_indexes(Tab, Old_opts, New_opts) ->
    Old_ixs = get_all_values(index, Old_opts),
    New_ixs = get_all_values(index, New_opts),
    Old = sets:from_list(Old_ixs),
    New = sets:from_list(New_ixs),
    Cut = sets:intersection(Old, New),
    Del = sets:subtract(Old, Cut),
    Add = sets:subtract(New, Cut),
    %% or reverse their order
    [{mnesia, add_table_index, Tab, Attr}
     || Attr <- sets:to_list(Add) ] 
	++
    [{mnesia, del_table_index, Tab, Attr}
     || Attr <- sets:to_list(Del) ]
    .
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Copy set/ordered_set to other table type
%%
%% Uses sticky_write to signal we need to lock the table for a longish
%% time; however, note that normally Tab1 is a freshly created
%% unshared table, so write collisions should never occur.
%%
%% - refactor: should this code be in schematool_table?

copy_table(Tab0, Tab1) ->
    mnesia:foldl(
      fun(Rec, Acc) ->
	      mnesia:write(Tab1, Rec, sticky_write),
	      0
      end,
      0,
      Tab0).

%% Copy bag table to set/ordered_set
%%
%% In this case, we run the risk of losing data (since a bag
%% can contain multiple records per key). We thus support 
%% collision policies to decide what to do for such cases.
%% (The simplest policy is just to choose one of the records,
%% for instance, the last encountered/written record.)
%%
%% - refactor: should this code be in schematool_table?

lossy_copy_table(keep_some_value, Tab0, Tab1) ->
    %% This option just overwrites any previous value,
    %% so it keeps one of the bag entries. (Which one
    %% depends on mnesia traversal order.)
    mnesia:foldl(
      fun(Rec, Acc) ->
	      mnesia:write(Tab1, Rec, sticky_write),
	      0
      end,
      0,
      Tab0);
lossy_copy_table(keep_first_value, Tab0, Tab1) ->
    %% if key already has a value, discard this record
    mnesia:foldl(
      fun(Rec, Acc) ->
	      %% UNFINISHED
	      %% normally, key to use is 2nd arg, but
	      %% I think mnesia is flexible; we will
	      %% need to pass the table options to handle
	      %% this
	      Key = element(2, Rec),
	      case mnesia:read(Tab1, Key, sticky_write) of
		  [] ->
		      mnesia:write(Tab1, Rec, sticky_write);
		  [_PrevRec] ->
		      ok
	      end
      end,
      0,
      Tab0);
lossy_copy_table(Other, _Tab0, _Tab1) ->
    exit({collision_policy_not_yet_handled, Other}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
