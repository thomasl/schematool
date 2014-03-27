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
%% This seems nice:
%%   ?MODULE:table(Tab, Xforms, Old, New)
%%
%% UNFINISHED
%% - when should transforms be done during a
%%   migration? also, do they run on all nodes?

-module(schematool_transform).
-export(
   [
    table/3,
    table/4,
    rec_kv/2,
    kv_rec/2,
    rec_def_of/1
   ]).

-include("schematool.hrl").

%-define(dbg(Str, Xs), io:format(Str, Xs)).
-define(dbg(Str, Xs), ok).

%% Record definitions look like:
%%   {RecName :: atom(), [ attr() ]}
%% where
%%   attr() ::= atom() | {atom(), any()}.
%%
%% See rec_def_of/1 for how to extract these from
%% schema, unless available from elsewhere

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Attribute transforms:
%%   fun(Old_KVs) -> Value
%% implicitly for {Attr, Default}:
%%   {Attr, fun(Old_KVs) -> proplists:get_value(Attr, Old_KVs, Default) end}
%%
%% This handles cases like "sum = field1+field2" though with an
%% unwieldy syntax.
%%
%% - special case: apply function to old attr value
%% - do we always want to use fun:s here?

%% Record transformers: multiple forms, probably
%% - old-rec => interim
%% - interim => interim
%% - interim => new-rec

%% Ordering of attribute and record transformers?
%% - defined by user?
%% - implicit?
%%
%% Special cases:
%% - permutation of attributes
%% - attrs deleted
%% - attrs added (with implicit)
%%
%% Would be nice to be able to COMPILE these, but maybe it's OK.

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

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

table(Tab, Old, New) ->
    table(Tab, [], Old, New).

%% Update the tabel to the new layout
%% 
%% NOTE: mnesia:transform_table/4 is implicitly a txn => do not wrap.
%%
%% UNFINISHED
%% - indexes updated elsewhere
%% - could call mnesia:transform_table/3 if New_rec unchanged
%%   (does that help?)

table(Tab, Xforms, Old, New = {New_rec, New_attr_dflts}) ->
    New_attrs = attr_names(New_attr_dflts),
    mnesia:transform_table(
      Tab, 
      fun(Rec) ->
	      KVs = rec_kv(Old, Rec),
	      NewKVs = xforms(Xforms, KVs),
	      NewRec = kv_rec(New, KVs),
	      NewRec
      end,
      New_attrs,
      New_rec).

%% UNFINISHED
%% - is this available elsewhere?

attr_names([{A, Dflt}|Xs]) ->
    [A|attr_names(Xs)];
attr_names([A|Xs]) when is_atom(A) ->
    [A|attr_names(Xs)];
attr_names([]) ->
    [].

%% xforms is a list of local/table transforms.
%%
%% {attr, A, PrevA}: copy A := PrevA
%% {attr, A, Fun}: Fun(Val, KVs) -> NewVal, replaces value of A
%% {rec, Fun}: Fun(KVs) -> NewKVs, updates the list of key-values
%%
%% (Global transforms {db, Fun} are specified at the schema level.)
%%
%% NOTE: attributes A are not restricted to attribute names,
%%  we can use them as temp variables too. But the final record
%%  is constructed from the new record definition's attributes.
%%
%% NOTE: simple attribute copying could be done at ZERO runtime cost
%%  by renaming attribute A to access the position of PrevA!
%%  - doesn't work if we are copying A := PrevA, A' := PrevA, etc
%%    but I think the 'rename' is a fairly common case
%%  - but alas, we don't have that level of control of records or
%%    their usage elsewhere, so we leave it for another time
%%
%% - might be useful to provide some helpers, e.g.
%%     {?MOD, sum, [attr1,...,attrN]}
%%   or whatnot

xforms(Xfs, KVs) ->
    lists:foldl(fun xform/2, KVs, Xfs).

xform({attr, A, PrevA}, KVs) when is_atom(PrevA) ->
    %% copy value, A := PrevA
    %% - useful when renaming an attribute
    V = proplists:get_value(PrevA, KVs, undefined),
    NewKVs = replace(A, V, KVs),
    NewKVs;
xform({attr, A, Fun}, KVs) ->
    %% Fun(KVs) -> Value
    NewV = app(Fun, KVs),
    NewKVs = replace(A, NewV, KVs),
    NewKVs;
xform({rec, Fun}, KVs) ->
    %% Fun(KVs) -> NewKVs
    NewKVs = app(Fun, KVs),
    NewKVs;
xform(Xf, _KVs) ->
    exit({unknown_table_xform, Xf}).

%% Replaces value of Attr with New, adding
%% the key-value if not found.

replace(Attr, New, [{Attr, _}|Xs]) ->
    [{Attr, New}|Xs];
replace(Attr, New, [X|Xs]) ->
    [X|replace(Attr, New, Xs)];
replace(Attr, New, []) ->
    [{Attr, New}].

%% Apply, F(KVs) -> NewKVs or NewVal
%%
%%  (NewVal when used by attr, NewKVs for rec)

app(F, KVs) when is_function(F) ->
    case catch F(KVs) of
	{'EXIT',_} ->
	    KVs;
	NewKVs ->
	    NewKVs
    end;
app({M, F, As}, KVs) ->
    %% or pass As as 1 arg?
    case catch apply(M, F, As ++ [KVs]) of
	{'EXIT',_} ->
	    KVs;
	NewKVs ->
	    NewKVs
    end.

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
    get_attr(Attr, Dflt, Xs);
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
	    case proplists:get_value(attributes, Opts, undefined) of
		undefined ->
		    exit({attributes_needed, Opts});
		Attrs ->
		    ?dbg("attrs ~p, raw rec ~p\n", [Attrs, RawRec]),
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

