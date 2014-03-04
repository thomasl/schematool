%%% File    : schematool_frags.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created :  7 Feb 2014 by Thomas Lindgren <>

%% Handling of fragmented tables.
%%
%% In essence, we have the following cases:
%% fragment 1 -> N (fragment table)
%% fragment N -> 1 (defragment table)
%% fragment N -> M (change frags)
%%
%% NOTE:
%% Not sure if fragmentation is changed by hand
%% often. In that case, the change needs to be reflected
%% in the schema, or perhaps should be routed
%% through schematool (updating the schema).
%%
%% use mnesia:activity to access
%% - should we have autocomputed accessors?
%%
%% 1-> N:
%%  1/ make table fragmented
%%   mnesia:change_table_frag(Tab, {activate, []})
%%   - also
%%   check the node pool with: mnesia:table_info(Tab, frag_properties)
%%  2/ add fragments
%%   Info = fun(Item) -> mnesia:table_info(Tab, Item) end,
%%   Dist = mnesia:activity(sync_dirty, Info, [frag_dist], mnesia_frag),
%%   mnesia:change_table_frag(Tab, {add_frag, Dist})
%%  3/ 
%%
%% From mnesia manual:
%% - use mnesia:change_table_frag/2 to add new fragments
%% - mnesia:add_table_copy/3, mnesia:del_table_copy/2, 
%%   mnesia:change_table_copy_type/2
%%   on each fragment to perform the actual redistribution
%%
%% (Does this also re-hash/re-distribute existing data?)
%%
%% Need more experience with fragmented tables ...
%% NOTE:
%% - in what situations do we require table copying?
%%   * if table copying is needed due to other reasons,
%%     we can shortcut the fragment migration too
%% - some migration MAY also be like
%%   1->N:
%%   copy A to A'
%%   delete A
%%   create A as fragmented table
%%   copy A' to A

-module(schematool_frag).
-export(
   [alter_node_pool/3,
    alter_fragments/4,
    adjust_fragments/3,
    declared_fragments/1
   ]).
%% dynamic/debugging stuff
-export(
   [info/2,
    frag_ctx/1
   ]).

%% LEARN MORE
%% Changing number of replicas:
%%   n_ram_copies, etc
%% man sez: Apply changes to "first fragment" (= first node?)
%% with add_table_copy etc, these will propagate
%% to the other fragments
%%
%% UNFINISHED
%% - handle primary keys (restricting deletion etc)
%%   * how do we add/remove/change primary/foreign keys dynamically?
%%   * various foreign tables, etc
%% - what is the "first fragment" or "first fragment name"?
%%   is this just the original table? no explanation found ...
%% - perform diff of frag_properties
%%   * also needs changes in schematool to invoke
%%     (and detect)
%% - changing storage mode of fragmented tables: "first fragment"
%%   * details?
%% - how are fragments replicated? are they replicated?
%%   * details on how to change replication?
%% - we have added a schematool 'fragments' option, is this needed?
%%   (use frag_properties)

%% Returns
%%   same       (no update needed)
%%   needs_copy (table can't be adjusted, needs to be copied)
%%   ...        (adjust table)

diff(Opts0, Opts1) ->
    Fr0 = proplists:get_value(frag_properies, Opts0, undefined),
    Fr1 = proplists:get_value(frag_properies, Opts1, undefined),
    if
	Fr0 == Fr1 ->
	    same;
	true ->
	    %% what can we figure out here?
	    %% - number of frags differ
	    needs_copy
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Working with nodes
%%
%% NB: I think the node_pool restricts (either directly or
%% heuristically) where fragments are placed. Not sure if there's any
%% other use at the moment.
%% 
%% Note: each change_table_frag/2 is a new transaction, so if we add M
%% nodes and delete N nodes, we will do M+N transactions.

alter_node_pool(Old_pool, New_pool, Actions) ->
    {Added, _Remain, Deleted} = schematool_nodes:diff(Old_pool, New_pool),
    if_nonempty(
      Added,
      {schematool_helper, add_pool_nodes, [Tab, Added]},
      if_nonempty(
	Deleted,
	{schematool_helper, delete_pool_nodes, [Tab, Deleted]},
	Actions)).

if_nonempty([], _Action, Actions) ->
    Actions;
if_nonempty(_, Action, Actions) ->
    [Action|Actions].

%% Do NOT run this in transaction, change_table_frag/2 already does

add_pool_nodes(Tab, Nodes) ->
    lists:foreach(
      fun(Node) ->
	      ok = atomic(mnesia:change_table_frag(Tab, {add_node, Node}))
      end,
      Nodes).

%% Do NOT run this in transaction, change_table_frag/2 already does

delete_pool_nodes(Tab, Nodes) ->
    lists:foreach(
      fun(Node) ->
	      ok = atomic(mnesia:change_table_frag(Tab, {del_node, Node}))
      end,
      Nodes).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Alter the fragmentation status and number of fragments
%% of the table.
%%
%% This is done by the schematool option {fragments, N}.
%% N = 0 means the table is not fragmented, otherwise
%% specifies N fragments.
%%
%% Automatically activates/deactivates fragmentation too.
%% - or maybe the adjust call should be a helper?
%%
%% NOTE: we can remove ALL fragments by iterating
%%  del_frag until it returns {no_exists, Tab}
%%  after which point it can be deactivated
%%
%% NOTE: Running this can be expensive, since each
%%  add/del operation means rehashing. Adding or deleting more
%%  than one fragment can be costly.

alter_fragments(Tab, Old, New, Actions) ->
    Old_N = declared_fragments(Old),
    New_N = declared_fragments(New),
    if
	Old_N =:= New_N ->
	    %% unchanged, do nothing
	    Actions;
	true ->
	    [{schematool_helper, adjust_fragments, [Tab, Old_N, New_N]}
	     |Actions]
    end.

%% Find the declared number of fragments for the table from
%% its options.
%% 
%% UNFINISHED
%% - if n_fragments not given, we assume 1 (no documentation given)
%% - F not used (yet)

declared_fragments(Opts) ->
    _F = proplists:get_value(fragments, Opts, 0),
    case proplists:get_value(frag_properties, Opts, undefined) of
	undefined ->
	    %% assume not fragmented
	    0;
	FragProps ->
	    Dflt = 1,
	    N = proplists:get_value(n_fragments, FragProps, Dflt),
	    N
    end.

%% adjust_fragments(Tab, Old_num_frags, New_num_frags) -> ok
%%
%% Actually adjust the number of fragments. We assume
%% the values have been retrieved elsewhere, that the tables
%% comply with these values, etc.
%%
%% A value of 0 means not fragmented.
%% Values of 1+ means 1 or more fragments.
%% Less than 0 is not allowed.
%%
%% Note that activating a non-fragmented table means
%% you get 1 fragment. (It seems.)

adjust_fragments(Tab, 0, 0) ->
    %% table is, and remains, unfragmented
    ok;
adjust_fragments(Tab, Old, 0) when Old > 0 ->
    %% delete all Old-1 fragments, then deactivate
    del_frags(Old-1, Tab),
    mnesia:change_table_frag(Tab, deactivate);
adjust_fragments(Tab, 0, New) when New > 0 ->
    %% activate, then add New-1 fragments
    ok = mnesia:change_table_frag(Tab, {activate, []}),
    add_frags(New-1, Tab);
adjust_fragments(Tab, Old, New) when Old =:= New ->
    %% unchanged
    ok;
adjust_fragments(Tab, Old, New) when Old > New ->
    %% delete some fragments
    del_frags(Old-New, Tab);
adjust_fragments(Tab, Old, New) when Old < New ->
    %% add some fragments
    add_frags(New-Old, Tab).

%% Delete N fragments

del_frags(N, Tab) when N > 0 ->
    del_frag(Tab),
    del_frags(N-1, Tab);
del_frags(N, Tab) when N =< 0 ->
    ok.

%% Delete one fragment

del_frag(Tab) ->
    mnesia:change_table_frag(Tab, del_frag).

%% Add N fragments, this is done in a loop
%% using the successive Dist values. (Urk.)
%%
%% (Also note the awkward mnesia interface: we can't run change_table_frag/2
%% inside a txn since it's a txn itself. But table_info/2 MUST on
%% the other hand be inside one. So add_frags/2 requires 2N txns to
%% add N fragments. Better solutions are welcome.)

add_frags(N, Tab) when N > 0 ->
    Dist0 = frag_ctx(fun() -> mnesia:table_info(Tab, frag_dist) end),
    io:format("Adding fragment to ~p\n", [Dist0]),
    ok = atomic(mnesia:change_table_frag(Tab, {add_frag, Dist0})),
    add_frags(N-1, Tab);
add_frags(N, _Tab) when N =< 0 ->
    ok.

atomic({atomic, Res}) ->
    Res;
atomic(Else) ->
    exit(Else).

%% Repeat Fun N times (if N > 0)

repeat(N, Fun) when N =< 0 ->
    ok;
repeat(N, Fun) ->
    Fun(),
    repeat(N-1, Fun).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Check if table copying needed
%% - optimize copy operation?

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Run in context to get mnesia_frag properties

info(Tab, Prop) ->
    frag_ctx(fun() -> mnesia:table_info(Tab, Prop) end).

frag_ctx(Fun) ->
    frag_ctx(Fun, []).

frag_ctx(Fun, Args) ->
    %% (use sync_dirty because it's used in examples...)
    mnesia:activity(sync_dirty, Fun, Args, mnesia_frag).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

ensure_nodes([N|Ns]) ->
    case net_adm:ping(N) of
	pong ->
	    ensure_nodes(Ns);
	pang ->
	    exit({node_not_reachable, N})
    end;
ensure_nodes([]) ->
    ok.

