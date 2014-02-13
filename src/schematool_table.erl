%%% File    : schematool_table.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 29 Jan 2014 by Thomas Lindgren <>

%% Altering a table definition. This is done
%% given the old and new definitions.
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
    without_schematool_options/1
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
%% UNFINISHED
%% - what is the correct order of actions?
%%   e.g., change table opts before or after layout, etc
%%   * correctness
%%   * optimization (change layout in ram before
%%     going to disc...? or other way around)
%%   * should we specify dependences and sort?

alter_table({Tab, Old_opts, New_opts}=TabDiff) ->
    Acts0 = alter_table_options(TabDiff),
    Acts1 = alter_table_layout(Tab, Old_opts, New_opts, []),
    Acts2 = alter_storage_type(Tab, Old_opts, New_opts, []),
    Acts3 = alter_indexes(Tab, Old_opts, New_opts),
    [Acts0, Acts1, Acts2, Acts3].

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
	      %% Handled elsewhere
	      Actions;
	 ({ram_copies, Ns}, Actions) ->
	      %% Handled elsewhere
	      Actions;
	 ({disc_only_copies, Ns}, Actions) ->
	      %% Handled elsewhere
	      Actions;
	 ({attributes, Attrs}, Actions) ->
	      %% Handled elsewhere
	      Actions;
	 ({record_name, Rec}, Actions) ->
	      %% Handled elsewhere
	      Actions;
	 ({index, Ixs}, Actions) ->
	      %% Handle this case in a second pass, there can
	      %% be several indexes declared. Skip here.
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
		      [{error, {unable_to_change, Tab, Opt}}|Actions];
		  true ->
		      Actions
	      end;
	 ({local_content, Bool}=Opt, Actions) ->
	      %% NB: no apparent way to change this property
	      Old = get_value(local_content, Old_opts, false),
	      if
		  Bool =/= Old -> 
		      [{error, {unable_to_change, Tab, Opt}}|Actions];
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

%% 

without_schematool_options(Opts) ->
    [ Opt || Opt <- Opts,
	     not schematool_option(Opt) ].

schematool_option({record, _Rec}) ->
    true;
schematool_option({transform, _Xf}) ->
    true;
schematool_option({transform, _Vsn, _Xf}) ->
    true;
schematool_option(_) ->
    false.

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
%% - fourth is actual change
%%
%% UNFINISHED
%% - are cases 2/3 errors instead? investigate

alter_storage(_Tab, {_Node, {Type, Type}}) ->
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
