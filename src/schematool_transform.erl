%%% File    : schematool_transform.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  7 Feb 2014 by Thomas Lindgren <>

%% Transforming tables
%%
%% Transformation is often needed when changing
%% the table attributes (fairly common).
%%
%% In ordinary erlang, this can be problematic,
%% since record definitions will often clash. We
%% provide helper functions to simplify writing
%% the migration.
%% - these helpers will probably be provided in
%%   schematool_helpers or suchlike
%%
%% Also assistance with running the table transforms
%% as required.
%%
%% UNFINISHED
%% - specify transforms
%% - perform transforms
%% - when should transforms be done during a
%%   migration? also, do they run on all nodes?

-module(schematool_transform).
-export(
   [
    rec_kv/2,
    kv_rec/2,
    rec_def_of/1
   ]).

-include("schematool.hrl").

%% Record definitions look like:
%%   {RecName :: atom(), [ attr() ]}
%% where
%%   attr() ::= atom() | {atom(), any()}.
%%
%% See rec_def_of/1 for how to extract these from
%% schema, unless available from elsewhere

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Extract the 'transform' keys and use them to build
%% transformation actions
%% 
%% UNFINISHED
%% - very very early
%% - too simplistic
%%   * also extract transforms associated with table def
%%   * attribute transforms
%% - should transforms be associated with schema versions
%%   (e.g., only select those mapping (old -> new) or
%%   those "coming from old_vsn")

schema(#schematool_info{schema=Schema}) ->
    [ {mnesia, transform_table, [Fun, Tab]}
      || {transform, Tab, Fun} <- Schema ] ++
	[ {schematool_helper, transform_table, [Fun, Tab]}
	  || {table, Tab, Opts} <- Schema,
	     {transform, Tab1, Fun} <- Opts, Tab1 == Tab ].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% UNFINISHED
%% - more flexibility in how to transform needed
%%   * spec per attribute transformation
%%   * spec ...

transform_record(Fun, OldRecDef, OldRec, NewRecDef) ->
    KVs = rec_kv(OldRecDef, OldRec),
    NewKVs = Fun(KVs),
    NewRec = kv_rec(NewRecDef, NewKVs),
    NewRec.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% One of the problems of transforming the records of a table
%% from one type to another is that record definitions often clash.
%% For example, adding a field to a record means you now have two
%% record definitions with the same name, which will cause preprocessor
%% issues.
%%
%% Here, we instead, provide tools to convert a record to key-value pairs,
%% and key-value pairs to a record. The simplest transformation is then:
%%
%%   1/ KVs    = rec_kv(Old_rec_def, OldRec)
%%   2/ NewRec = kv_rec(New_rec_def, KVs)
%%
%% The *_rec_def terms are defined as above. Basically, they should
%% be automatically extracted from the schema. 
%% - this can be done by looking at each table and its options.

%% Given a record definition and a record, return list of key-value pairs.

rec_kv({Rec, Attrs}, R) ->
    case tuple_to_list(R) of
	[Rec|Vals] ->
	    lists:zip( [ attr_of(Attr) || Attr <- Attrs ],
		       Vals );
	_ ->
	    exit({bad_record, {Rec, Attrs}, R})
    end.

%% Given a record definition and a key-value list, return the
%% corresponding record.

kv_rec({Rec, Attrs}, KVs) ->
    Vals = [ get_attr(Attr, KVs) || Attr <- Attrs ],
    list_to_tuple([Rec|Vals]).

%% Note: the Dflt must already be computed, so there
%%  may be some problematic record definitions (since
%%  the field value in a record def can be a weird
%%  syntax fragment). Here, we instead expect that record
%%  definitions are "well-formed".

get_attr({Attr, Dflt}, KVs) ->
    get_attr(Attr, Dflt, KVs);
get_attr(Attr, KVs) when is_atom(Attr) ->
    Dflt = undefined,
    get_attr(Attr, Dflt, KVs).

get_attr(Attr, Dflt, [{Attr, Val}|_]) ->
    Val;
get_attr(Attr, Dflt, [_|Xs]) ->
    get_attr(Attr, Xs);
get_attr(Attr, Dflt, []) ->
    Dflt.

%% Attributes are stored either as just the atom
%% or as a pair {Attr, DefaultValue}. (In the first
%% case, the default value is the atom 'undefined',
%% as usual.)

attr_of({Attr, _Dflt}) ->
    Attr;
attr_of(Attr) when is_atom(Attr) ->
    Attr.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Extract a record definition from a table spec.
%%
%%  {table, Tab, Opts}
%%
%% If Opts contains {record, #rec{}}, we use that.
%% Otherwise, extract what info we can from the other options.
%%
%% Note: to do this, we need BOTH of these in Opts:
%%
%%    {attributes, record_info(fields, rec)}
%%    {record, #rec{}}
%% 
%% Defaults (per mnesia man page): 
%% - if record_name is not given, use Tab
%% - if attributes is not given, use [key,value]
%%
%% UNFINISHED
%% - default values NOT AVAILABLE unless record option
%%   is shown
%%   * warning?

rec_def_of({table, Tab, Opts}) ->
    case proplists:get_value(record, Opts, undefined) of
	undefined ->
	    %% In this case, default values cannot be recovered
	    %% - warning?
	    Rec = proplists:get_value(record_name, Opts, Tab),
	    AttrDefs = proplists:get_value(attributes, Opts, [key,value]),
	    {Rec, AttrDefs};
	RawRec ->
	    case proplist:get_value(attributes, Opts, undefined) of
		undefined ->
		    exit({attributes_needed, Opts});
		Attrs ->
		    [Rec|RawAttrs] = tuple_to_list(RawRec),
		    AttrDefs =
			[ attr_def(Zip) || Zip <- lists:zip(Attrs, RawAttrs) ],
		    {Rec, AttrDefs}
	    end
    end.

%% (if default is undefined, drop it)

attr_def({Attr, undefined}) ->
    Attr;
attr_def({Attr, Default}=Def) ->
    Def.

%% Mainly for testing

rec_defs_of(#schematool_info{schema=Schema}) ->
    [ rec_def_of(Table) || {table, _Tab, _Opts} = Table <- Schema ].

