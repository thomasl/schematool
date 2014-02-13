%%% File    : schematool_xf.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 13 Feb 2014 by Thomas Lindgren <>

%% Library of helpful attr, rec and db transforms

-module(schematool_xf).
-export(
   [
   ]).

%% Val is original value of attribute
%% Attrs is list of items to sum
%% KVs is the key-value list
%%
%% Examples:
%%  {attr, attrC, {?MOD, sum, [attrA, attrB]}}
%%    attrC := attrA + attrB
%%  {attr, attrC, {?MOD, sum, [attrC, 1]}}
%%    attrC := attrC+1 (increment)
%%  {attr, attrC, {?MOD, sum, [attrA]}}
%%    attrC := attrA (copy)
%%    however, idiomatic usage is {attr, attrC, attrA}
%%    - this is special-cased
%%
%% (Should we have a "generic arithmetic thing"? The
%% user can write that, though.)

sum(Attrs, Val, KVs) ->
    lists:foldl(
      fun(C, Acc) when is_integer(C) ; is_float(C) ->
	      C+Acc;
	 (A, Acc) when is_atom(A) ->
	      get_value(A, KVs) + Acc
      end,
      0, 
      Attrs).

%% Returns 0 if not found or not a number.

get_value(A, KVs) ->
    case lists:keyfind(A, 1, KVs) of
	N when is_integer(N) ; is_float(N) ->
	    N;
	_ ->
	    0
    end.

%% const: set attribute to constant
%%
%% Example: set last_updated to same current time everywhere
%%   {attr, last_updated, {?MOD, const, [datetime()]}}
%%
%% (the function datetime() is evaluated once before the
%% transform)

const([Const], _Val, _KVs) ->
    Const.

