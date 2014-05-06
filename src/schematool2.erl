%%% File    : schematool2.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  2 May 2014 by Thomas Lindgren <>

%% NOTE: use schematool.erl as the entrypoint.
%%
%% We have, in essence:
%% - table added
%% - table deleted
%% - table changed
%%
%% New table migration thing, based on table copying.  Failsafe
%% approach: table copying (perhaps twice).  Try to detect situations
%% when you can do better. In some cases, you can just flip some
%% options or add/del indexes or table copies. In other cases, like a
%% table layout change, you can do a mnesia table rewrite. In the
%% worst case, you will have to copy the table twice.
%%
%% Copying twice is needed because we usually need to keep the table
%% name, and there is no mnesia rename operation. (Sigh.)
%%
%% The copy operation is staged depending on what is being done. In
%% essence, we want to minimize the cost in time and space. The
%% approach should be dynamically decided based on table size.
%%
%% Schematool should perhaps provide a way to DYNAMICALLY resolve
%% table names to tables. That way, the second copy is not needed
%% (just redirect the name to the new table).

-module(schematool2).

%% What are the dimensions that may change for a table?
%% - table nodes
%% - fragmentation
%% - layout
%% - indexes
%% - type, storage type
%% - simple options
%% - difficult options [need copying]
%%
%% Typical worst case (Src -> Dst):
%% 1/ create Tmp, compatible with leastcost(Src, Dst)
%% 2/ copy all records from Src to Tmp
%% 3/ create Dst (if Src = Dst, delete Src then recreate)
%% 4/ copy all records from Tmp to Dst
%% 5/ delete Tmp
%%
%% Table planning:
%% - should Tmp be compatible with Src or Dst?
%% - what does "compatible" mean?
%%   * "bare bones" to hold records cheaply and cheerfully
%%   * represent records faithfully
%%     = either old or new layout
%%   * any storage option, ram_copies should be fastest
%%   * no extra indexes
%%   * type: ... ("compatible")
%%   * table nodes: use a single node? 
%%   * simple and difficult options: skip as many as possible
%% - when to do layout transformation?
%% - when to create indexes?
%%
%% Fragmentation handling? This can apparently be done
%% on the fly, see the existing table.
%%
%% Special cases:
%% - transform layout
%% - change options of existing table
%% - both layout and options
%% - add or delete table copy

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Return what strategy is needed
%% - full copy
%% - layout transform
%% - options change
%% - layout + options
%%
%% Layout change is defined by record name and attributes (and defaults).
%%
%% Options decide the others:
%% - worst case, full copy
%% - otherwise, possibly empty options change
%%
%% UNFINISHED
%% - if full_copy = full_copy, we need to emit something useful
%%   (otherwise, just do the rest)
%% - if layout = true, return a transform spec and insert it
%%   into the list of others appropriately

analyze_options(Tab, Old_opts, New_opts) ->
    Full_copy = full_copy_needed(Tab, Old_opts, New_opts),
    Layout = layout_transform(Tab, Old_opts, New_opts),
    case Full_copy of
	full_copy ->
	    %% Layout
	    lists:flatten(
	      [{schematool_helper, full_copy, [Tab, Old_opts, New_opts]},
	       Layout]);
	no_copy ->
	    Storage = table_storage_changes(Tab, Old_opts, New_opts),
	    Minors  = table_minor_options(Tab, Old_opts, New_opts),
	    Indexes = table_index_changes(Tab, Old_opts, New_opts),
	    Layout ++ Storage ++ Minors ++ Indexes
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Check the new options to see if they are compatible
%% with the old ones.

full_copy_needed(Tab, Old_opts, New_opts) ->
    Type = table_type(Tab, Old_opts, New_opts),
    Majors = table_major_options(Tab, Old_opts, New_opts),
    case {Type, Majors} of
	{no_copy, no_copy} ->
	    no_copy;
	{full_copy, _} ->
	    full_copy;
	{_, full_copy} ->
	    full_copy
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Check if table type has changed. 
%%
%% Changing the table type requires a full table copy (as far as
%% I'm aware).

table_type(Tab, Old_opts, New_opts) ->
    Old_T = proplists:get_value(type, Old_opts, set),
    New_T = proplists:get_value(type, New_opts, set),
    if
	Old_T == New_T ->
	    no_copy;
	true ->
	    full_copy
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Check the minor options (which can be changed with mnesia operations
%% without copying)
%%
%% UNFINISHED
%% - mnesia does not document the default 'majority' value, verify!

table_minor_options(Tab, Old_opts, New_opts) ->
    LoadOrder = 
	case same_or_new(table_options(load_order, Old_opts, New_opts, 0)) of
	    same ->
		[];
	    {new, NewN} ->
		[{mnesia, change_table_load_order, [Tab, NewN]}]
	end,
    Majority = 
	case same_or_new(table_options(majority, Old_opts, New_opts, false)) of
	    same ->
		[];
	    {new, NewMaj} ->
		[{mnesia, change_table_majority, [Tab, NewMaj]}]
	end,
    AccessMode = 
	case same_or_new(table_options(access_mode, Old_opts, New_opts, read_write)) of
	    same ->
		[];
	    {new, NewAcc} ->
		[{mnesia, change_table_access_mode, [Tab, NewAcc]}]
	end,
    lists:flatten([LoadOrder, Majority, AccessMode]).

same_or_new({Val, Val}) ->
    same;
same_or_new({_, NewVal}) ->
    {new, NewVal}.

%% The major options return 'full_copy' if table copy is required, 'no_copy' otherwise.
%%
%% UNFINISHED
%% - StoreProps should probably ALSO be sorted INSIDE the list, see mnesia:create_table
%%   for format

table_major_options(Tab, Old_opts, New_opts) ->
    LocalContent = table_options(local_content, Old_opts, New_opts, false),
    SNMP = table_options(snmp, Old_opts, New_opts, []),
    StoreProps = table_options(storage_properties, Old_opts, New_opts, []),
    case same(LocalContent) andalso same_sorted(SNMP) andalso same_sorted(StoreProps) of
	true ->
	    no_copy;
	false ->
	    full_copy
    end.

same({Val, Val}) ->
    true;
same(_) ->
    false.

same_sorted({A, B}) ->
    same({lists:sort(A), lists:sort(B)}).

%%

table_options(Opt, Opts0, Opts1) ->
    Dflt = undefined,
    table_options(Opt, Opts0, Opts1, Dflt).

table_options(Opt, Opts0, Opts1, Dflt) ->
    Val0 = proplists:get_value(Opt, Opts0, Dflt),
    Val1 = proplists:get_value(Opt, Opts1, Dflt),
    {Val0, Val1}.
	    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Add/delete indexes based on index specs.
%% 
%% NB: this is a bit weird, I think mnesia works like this:
%%   {index, [a,b,c]} creates 3 index tables, based respectively on
%% attributes a, b, and c. (Ie, Does not create a compound index on the three
%% attrs.)
%%
%% UNFINISHED
%% - assume there can be several index declarations, is this ok?
%% - verify mnesia behaviour when creating table

table_index_changes(Tab, Old_opts, New_opts) ->
    Ix0 = sets:from_list(lists:flatten(proplists:get_all_values(index, Old_opts))),
    Ix1 = sets:from_list(lists:flatten(proplists:get_all_values(index, New_opts))),
    Del = sets:subtract(Ix0, Ix1),
    Add = sets:subtract(Ix1, Ix0),
    [ {mnesia, add_table_index, [Tab, Attr]} || Attr <- sets:to_list(Add) ] ++
    [ {mnesia, del_table_index, [Tab, Attr]} || Attr <- sets:to_list(Del) ].
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Compute the storage changes given the old and new options.
%% 
%% Implementation note:
%% - collect two dicts of {Old_storage, New_storage} items
%%   (with 'undefined' value depending on whether old or new options)
%% - merge the options => those that occur in both will get old and
%%   new types, while those occurring in one will get type undefined
%%   either as old or new
%% - decide on what to do given {Old_storage, New_storage}
%%
%% UNFINISHED
%% - what if all table copies have disappeared? do we need to
%%   delete_table as well as del_table_copy?
%%   * normally will be detected because of missing definition, right?
%%   * test what happens ...

table_storage_changes(Tab, Old_opts, New_opts) ->
    Store0 = table_storage_types(Old_opts),
    Store1 = table_storage_types(New_opts),
    D0 = dict:from_list([ {N, {StN, undefined}} || {N, StN} <- Store0 ]),
    D1 = dict:from_list([ {N, {undefined, StN}} || {N, StN} <- Store1 ]),
    D = dict:merge(
	  fun(Key, Val0, Val1) ->
		  merge_vals(Val0, Val1)
	  end,
	  D0,
	  D1),
    Store = dict:to_list(D),
    safe_order(storage_changes(Tab, Store)).

%% Returns [{Node, StorageType}] (sorted, for easy comparison)
%%
%% We can use add_table_copy/3, del_table_copy/2, change_table_copy_type/3
%%
%% UNFINISHED
%% - what is the default? ie, if none of these is given

table_storage_types(Opts) ->
    Disc_nodes = proplists:get_value(disc_copies, Opts, []),
    Ram_nodes = proplists:get_value(ram_copies, Opts, []),
    Disc_only_nodes = proplists:get_value(disc_only_copies, Opts, []),
    lists:sort(
      [ {Node, disc_copies} || Node <- Disc_nodes ]
      ++ [ {Node, ram_copies} || Node <- Ram_nodes ]
      ++ [ {Node, disc_only_copies} || Node <- Disc_only_nodes ]
     ).

%%

merge_vals({St0, undefined}, {undefined, St1}) ->
    {St0, St1}.

%% Safe order of operations: in particular, we MUST ensure that
%% some table copies remain at all times
%%
%% Current approach: sort the list, which means del_table_copy
%% items end up last.
%%
%% UNFINISHED
%% - MUST sort operations so that some copies remain (del_copy last?)
%% - pretty fragile! rewrite!

safe_order(Xs) ->
    lists:sort(Xs).

%% We get as input a list of {Node, {Old_store, New_store}} items.
%% We now compute the changes to be done.
%% - if Old_store = undefined, add a table copy of type New_store
%% - if New_store = undefined, delete the table copy
%% - if Old_store =/= New_store, change_table_copy
%% - otherwise, no change
%%
%% We emit a list of changes to be done (mnesia calls).
%%
%% UNFINISHED
%% - protect operations with txn? check if needed/available
%% - do layout transforms inside this sequence?
%%   * better to do layout xform on minimum number of copies?
%%   * better to do layout xform on fastest storage of copies?

storage_changes(Tab, [{Node, {undefined, NewSt}}|Xs]) ->
    [ {mnesia, add_table_copy, [Tab, Node, NewSt]} | storage_changes(Tab, Xs)];
storage_changes(Tab, [{Node, {OldSt, undefined}}|Xs]) ->
    [ {mnesia, del_table_copy, [Tab, Node]} | storage_changes(Tab, Xs)];
storage_changes(Tab, [{Node, {OldSt, NewSt}}|Xs]) when OldSt == NewSt ->
    storage_changes(Tab, Xs);
storage_changes(Tab, [{Node, {OldSt, NewSt}}|Xs]) when OldSt =/= NewSt ->
    [ {mnesia, change_table_copy_type, [Tab, Node, NewSt]} | storage_changes(Tab, Xs) ];
storage_changes(_Tab, []) ->
    [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Layout transform is needed when the record layout has changed,
%% i.e., either the record name or the sequence of attributes
%% (= order or number of attrs has changed).
%%
%% NOTE: we assume that we do not need to transform existing
%% attributes when the default value has changed. This is handled
%% by comparing just the attr names lists, rather than the attr list
%% (which also contains default values).
%%
%% If the lists differ, schematool_transform:table/N will only
%% fill in the defaults if the value is missing, ie the attribute is
%% new.
%%
%% NOTE: Currently no handling of attribute value transforms.
%%  Invoke schematool_transform:table/4 to add such transforms.

layout_transform(Tab, Opts0, Opts1) ->
    Rec0a = {R0, As0} = rec_info(Tab, Opts0),
    Rec1a = {R1, As1} = rec_info(Tab, Opts1),
    Rec0 = {R0, attr_names_list(As0)},
    Rec1 = {R1, attr_names_list(As1)},
    if
	Rec0 == Rec1 ->
	    [];
	true ->
	    %% UNFINISHED
	    %% - make sure the Rec1a attr defaults do not overwrite
	    %%   existing records, just use it if attr is new
	    %% - should probably be done in schematool_transform:table/N
	    [{schematool_transform, table, [Tab, Rec0a, Rec1a]}]
    end.

%% 

rec_name({RecName, _AttrInfo}) ->
    RecName.

%% Get attribute names from a rec info structure

attr_names({_RecName, AttrInfo}) ->
    attr_names_list(AttrInfo).

attr_names_list(AttrInfo) ->
    [ if
	  is_atom(A) ->
	      A;
	  is_tuple(A) ->
	      element(1, A)
      end || A <- AttrInfo ].

%% Return {RecName, [AttrInfo]}
%% where AttrInfo = AttrName | {AttrName, Default}

rec_info(Tab, Opts) ->
    AttrNames = proplists:get_value(attributes, Opts, [key, value]),
    RecName = proplists:get_value(record_name, Opts, Tab),
    Attrs =
	case proplists:get_value(record, Opts, undefined) of
	    undefined ->
		AttrNames;
	    Rec when is_tuple(Rec) ->
		[_RecName|AttrDefaults] = tuple_to_list(Rec),
		semi_zip(AttrNames, AttrDefaults);
	    Other ->
		exit({option_value_not_handled, {record, Other}})
	end,
    {RecName, Attrs}.

%% If attr default is 'undefined', we just provide the attr name.
%% Otherwise, provide {Name, DefaultValue}.

semi_zip([A|As], [undefined|Ds]) ->
    [A|semi_zip(As, Ds)];
semi_zip([A|As], [D|Ds]) ->
    [{A, D}|semi_zip(As, Ds)];
semi_zip([], []) ->
    [].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% We need to split this into two:
%% - migration planner
%%   = decide how to do the migration (cost-based)
%% - migration executor
%%   = basically library functions to do the migration

%% Transform table
%% - handle layout change

%% Copy from one table to another
%% - the tables are already created
%% - the copying may involve layout changes
%%
%% Migration planner should make sure the copying is least cost.

%% Create temporary table compatible with options

%% Create new table

%% Delete table


