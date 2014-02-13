%% -*- Erlang -*-

%% schematool metadata
-record(schematool_info, {datetime, schema}).

%% schematool changes
%%   {migrate, KeyA, KeyB}
%%   {create, Key}
%%   {delete, Key}
%%   {load, Key}

-record(schematool_changelog, {datetime, action}).
