%%% File    : schematool_verify.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 25 Mar 2014 by Thomas Lindgren <>

%% Verify the current schema against the database.
%%
%% Construct a schema from the current tables and compare to a schematool schema.
%%
%% - can also be used to construct an initial schema

-module(schematool_verify).
-export(
   []
  ).

%% Check if schematool installed
%%
%% Lookup options for all the tables except the 'meta' tables
%% - mnesia schema, schematool_*
%%
%% If fragmented, lookup properties for those as well.
%% For schematool tables, lookup in particular the nodes used.
%% Schematool options are of course not available at the mnesia level.

implicit_schema() ->
    %% get list of tables
    %% for each table of interest
    %% - options per table
    %% - incl fragment info
    %% 
    exit(nyi).
