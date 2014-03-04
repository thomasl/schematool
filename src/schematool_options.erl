%%% File    : schematool_options.erl
%%% Author  : Thomas Lindgren <>
%%% Description : 
%%% Created : 13 Feb 2014 by Thomas Lindgren <>

%% Common lib to handle schematool options.

-module(schematool_options).
-export(
   [without/1,
    table_option/1
   ]).

%%  Used to get rid of schematool options so
%% that e.g., mnesia:create_table doesn't puke

without(Opts) ->
    [ Opt || Opt <- Opts,
	     not table_option(Opt) ].

%% These are the table options introduced by schematool

table_option({record, _Rec}) ->
    true;
table_option({transform, _Xf}) ->
    true;
table_option({transform, _Vsn, _Xf}) ->
    true;
table_option({fragments, _N}) ->
    true;
table_option(_) ->
    false.



