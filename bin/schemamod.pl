#!/usr/bin/perl
#
# We want to use records and includes in our
# but erlang's epp only works on modules (because it's a joke, see;
# Robert Virding, e.g., 
# http://erlang.org/pipermail/erlang-questions/2006-October/023409.html )
# ... so we need to generate a module
#
# Easiest way to do this is to wrap the info in
# a simple definition template.
#
# And to do that, we use ... perl. Yay!

## Usage:
##  perl schemamod.pl example.schema
##
## Writes module to example_schema.erl
## - note that if there are errors in example.schema,
##   the generated module will have errors too

########################################
## We define the schema as a template
## consisting of sections.
## 
## Each section is started by =name, which names it.
## There is no support for multiple occurrences of
## the same name. There is an implicit section for
## lines before the first header, called 'prologue'.
##
## UNFINISHED
## - predefined -import to make schema defs even simpler

use Getopt::Long;
use File::Basename qw(fileparse basename);
use strict;

## Options mangling

my $file;
my $outfile;
my $module;
my $force_overwrite = 0;

GetOptions(
    'schemafile=s' => \$file,
    'outfile=s' => \$outfile,
    'module=s' => \$module,
    'force' => \$force_overwrite
    );

unless ($file) {
    die "No schema file given (--schemafile file.schema)";
}

unless ($outfile) {
    die "No output file given (--outfile file.erl)";
}

unless ($module) {
    my $module_prefix = basename($file, ".schema");
    $module = $module_prefix."_schema";
}

unless (-f $file) {
    die "Schema file $file does not exist";
}

if (-e $outfile) {
    if ($force_overwrite) {
	# print STDERR "Overwriting existing $outfile\n";
    } else {
	die "Output file $outfile already exists";
    }
}

# print STDERR "Input: $file\nOutput: $outfile\nMod: $module\n";

## Main body of script

my @lines;

{
    # local $/ = undef;
    open FILE, $file or die "Couldn't open $file: $!";
    # binmode FILE;
    @lines = <FILE>;
    close FILE;
}
# print STDERR "Lines read: ".@lines."\n";

my @section_list;

## Split into sections
##
## A section looks like '^%%\s*=(\w+)' where
## the name is \w+
## and ends with the next section or end of file.
##  (% is the erlang comment character)
##
## There is an initial 'prologue' where all 
## lines before the first section line are
## collected.

my $curr_section = "prologue";
my @curr_lines;
my %sections;
my $num_lines = 0;

foreach my $line (@lines) {
    $num_lines += 1;
    if ($line =~ m!^%%=(\w+)!) {
	## Flush current section
	push @section_list, $curr_section;
	my $section = join("", @curr_lines);
	$sections{$curr_section} = $section;
	## Start new section
	$curr_section = $1;
	@curr_lines = ();
	$num_lines = 0;
	$section = "";
    } else {
	push @curr_lines, $line;
    }
}

## Flush last section
##  (can this be done inside the loop above?)

if ($num_lines > 0) {
    ## Flush remaining section
    push @section_list, $curr_section;
    my $section = join("", @curr_lines);
    $sections{$curr_section} = $section;
} else {
    # print STDERR "*** no final section, done\n";
}

########################################
## Phase 2: process the templates to build
##  the appropriate module etc

## augment prologue with required declarations

my $prologue = $sections{"prologue"};
$prologue = 
    $prologue.
    "\n-module($module).".
    "\n-export([schema/0]).".
    "\n\n";

$sections{"prologue"} = $prologue;

## generate the schema as a function

my $schemasec = $sections{"schema"};
unless ($schemasec) {
    die "Section 'schema' missing, unable to proceed\n";
}

$schemasec = "schema() -> \n".$schemasec."\n\n";
$sections{"schema"} = $schemasec;

########################################
## Phase 3: print the result

## We now have all the sections, print them
## in the order they appeared.

open (OUT, ">$outfile") or die "Unable to open $outfile: $!";
foreach my $key (@section_list) {
    print OUT "%% SECTION ".$key."\n";
    print OUT $sections{$key};
}

print OUT "%% END\n";
close OUT;
