%% -*- Erlang -*-

%% schematool metadata
%% - datetime is primary key, universal {YMD, HMS}
%% - vsn is undefined or a binary
%% - schema is the actual data
%% - origin is 'undefined' OR a pair
%%    {Filepath::binary(), Mtime::datetime()}
%%   (this can be used to see if two schemas with
%%   same version come from same file)
%%   * left undefined is okay at the time of writing

-record(schematool_info, {datetime, vsn, schema, origin}).

%% schematool changes
%%   {migrate, KeyA, KeyB}
%%   {create, Key}
%%   {delete, Key}
%%   {load, Key}
%%
%% datetime = universal datetime of action
%% action = one of the above, defined by schematool.erl

-record(schematool_changelog, {datetime, action}).
