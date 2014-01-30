%%% File    : schematool_table.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 29 Jan 2014 by Thomas Lindgren <>

%% Altering a table definition. This is done
%% given the old and new definitions.
%%
%% UNFINISHED
%% - fragmented tables NOT HANDLED
%% - a couple of options NOT HANDLED
%% - multiple storage types for a tables NOT HANDLED
%% - run the generated instructions

-module(schematool_table).
-export(
   [alter_table/1,
    alter_storage_type/4
   ]).

-import(proplists, 
	[get_value/3,
	 get_all_values/2
	]).

-define(dbg(Str, Xs), io:format(Str, Xs)).
%-define(dbg(Str, Xs), ok).

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

%% Returns a list of instructions
%% - perhaps with a priority so we can sort them
%%   into "good order"?

alter_table({Tab, Old_opts, New_opts}) ->
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
	 (Opt, Actions) ->
	      [{error, {unknown_option, Tab, Opt}}|Actions]
      end,
      [],
      New_opts
     ).

%% This function collects the following options:
%%   attributes,
%%   record_name,
%%   explicit table transform funs (schematool extension)
%%
%% Use:
%% mnesia:transform_table(Tab, Fun, New_attrs, New_rec)
%% mnesia:transform_table(Tab, Fun, New_attrs)
%%  Fun can be 'ignore' if we just change the namings
%%
%% UNFINISHED
%% - handling of fragmented tables here?

alter_table_properties(Tab, Old_opts, New_opts, Actions) ->
    OldRec = get_value(record_name, Old_opts, Tab),
    NewRec = get_value(record_name, New_opts, OldRec),
    Rec_name =
	if
	    OldRec == NewRec ->
		undefined;
	    true ->
		NewRec
	end,
    OldAttrs = get_value(attributes, Old_opts, [key, value]),
    NewAttrs = get_value(attributes, New_opts, [key, value]),
    Xforms = get_all_values(transforms, New_opts),
    transform_table_properties(Tab, Rec_name, OldAttrs, NewAttrs, Xforms, Actions).

%% Do the actual transforming
%%
%% UNFINISHED
%% - the Fun must do the appropriate permutation of attributes and/or initialize
%%   dead attributes, and run the Xforms (or run the Xforms first?)
%% - write a "permutation helper", given old record, new record, old+new attrs
%%   (using attr position to update)
%% - write an "attribute transform helper" (maybe)

transform_table_properties(Tab, NewRec, Old_attrs, New_attrs, Xforms, Actions) ->
    Fun = 
	case Xforms of
	    [] ->
		%% UNFINISHED - permute attrs
		ignore;
	    _ ->
		%% UNFINISHED
		{error, {run_xforms, Xforms}}
	end,
    case NewRec of
	undefined ->
	    if
		Old_attrs == New_attrs ->
		    %% nothing to be changed, yay
		    Actions;
		true ->
		    [{mnesia, transform_table, [Tab, Fun, New_attrs]}|Actions]
	    end;
	_ ->
	    [{mnesia, transform_table, [Tab, Fun, New_attrs, NewRec]}|Actions]
    end.

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
    Cut = sets:intersect(Old, New),
    Del = sets:subtract(Old, Cut),
    Add = sets:subtract(New, Cut),
    %% or reverse their order
    [{mnesia, add_table_index, Tab, Attr}
     || Attr <- sets:to_list(Add) ] 
	++
    [{mnesia, del_table_index, Tab, Attr}
     || Attr <- sets:to_list(Del) ]
    .
