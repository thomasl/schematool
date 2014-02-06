Schematool
==========

Helpful tools to generate, maintain and evolve a mnesia schema.

The mnesia schema is usually managed ad hoc inside various
erlang modules and scripts. Schematool provides a way to instead
manage the schema as a data structure.

STATUS: tool chain unfinished.

Installation
----------

Uses git submodules (git version > 1.8.2) to provide smart_exceptions.
Here is what you have to do to clone:

    git clone git@github.com:thomasl/schematool.git --recursive

or possibly

   git clone git@github.com:thomasl/schematool.git
   git submodule update --init --recursive

(Note: submodules are a bit new to me, so better approaches
are welcome.)

Usage
----------

Write a schema file, then run a script to create the schema (including
tables, etc).

First, the schema file must look like this:

    ...
    %%=preamble
    <include files, record definitions, etc>
    %%=schema
    <schema definition>

Each line '=foo' defines a section named 'foo'. In order to use the
tool, your code must be in the correct section, as detailed below. (If you know what
you're doing and are willing to possibly rewrite your schema
definition between releases, you can break these rules.)

- All attributes such as include, include_lib, record and
macros MUST be in the 'preamble' section. 
- The schema definition MUST be in the 'schema' section.
- As a convention, use file extension .schema.

Second, assuming that the path to the schematool git directory
is $SCHEMATOOL, create the schema as follows:

    $ perl $SCHEMATOOL/bin/schematool.pl --schemafile test.schema --einc . --ebin ../ebin

This creates a compiled schema module ../ebin/test_schema.beam (along with a "submodule" in the
same directory, which can be ignored). Next create the schema:

    $ perl $SCHEMATOOL/bin/create-schema.pl --schema myschema --node a@localhost

This should create the schema and mnesia tables needed. Start using the node:

    $ erl -pa ../ebin -name a@localhost
    ...
    1> mnesia:start().
    2> mnesia:info().

If your schema is using multiple nodes, you need to create it on all the nodes, start all the
nodes, and so on.

Future Extensions
----------

- schema/database migration
- autodefine accessors that utilize indexes, etc.
- define upgrade path for schema from vsn A to vsn B
- also define downgrade path back from B to A

Schema definition
==========

The schema definition is an erlang term (followed by a terminating
dot) describing the current schema in the jargon we will define below.
(*Note*: should it be a sequence of separated terms? e.g., one per table)

The schema definition describes what tables are to be defined
and the table attributes, including fields, type, indexes, and so on.

The schema definition (probably) should contain the list of nodes
to be used.

The schema definition may also be extended to contain other metadata in
order to define accessors, upgrade/downgrade and other library
functionality. 

(UNFINISHED)

Scripts
==========

- bin/schemamod.pl: given schema file, generate erlang schema module
- bin/create-schema.pl: given erlang schema module, create schema and tables
- src/*.erl: erlang files to further process the schema
- examples/*: usage examples (NB: need more work)
- test/*: tests (NB: empty at time of writing)
