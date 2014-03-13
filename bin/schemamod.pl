#!/usr/bin/perl
#
# We want to use records and includes in our
# but erlang's epp only works on modules (because it's a joke, see;
# Robert Virding, e.g., 
# http://erlang.org/pipermail/erlang-questions/2006-October/023409.html )
# ... so we need to generate a module to run epp easily
#
# Easiest way to do this is to wrap the info in
# a simple definition template.
#
# And to do that, we use ... perl. Yay!

## Usage: see usage() below
##
## Base usage:
## Writes module to example_schema.erl
## - note that if there are errors in example.schema,
##   the generated module will have errors too
## - keep track of current line to give good erlang error message

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
## - add predefined -import helpers to make schema defs even simpler

use Getopt::Long;
use File::Basename qw(fileparse basename);
use strict;

## Options mangling

my $file;
my $outfile;
my $module;
my $force_overwrite = 0;
my $print_vsnfile = 0;

GetOptions(
    'schemafile=s' => \$file,
    'outfile=s' => \$outfile,
    'module=s' => \$module,
    'force' => \$force_overwrite,
    'vsnfile' => \$print_vsnfile
    );

sub usage() {
    print STDERR
	"Usage:\n".
	"$0 --schemafile file.schema ...\n".
	"  --outfile file\n".
	"  --module modname\n".
	"  --force\n".
	"to generate the schema code, or\n".
	"$0 --schemafile file.schema --outfile file --vsnfile\n".
	"to get the implicit version file name, if needed\n".
	"";

    exit(1);
}

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
##
## $line_no keeps track of current line
## $section_line{$section} maps to the starting line of the section
##   (This is used to emit -file attributes for source compile errors)

my $curr_section = "prologue";
my @curr_lines;
my %sections;
my %section_line;
my $num_lines = 0;
my $line_no = 0;

foreach my $line (@lines) {
    $num_lines += 1;
    $line_no += 1;
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
	$section_line{$curr_section} = $line_no;
	# print STDERR "Section $curr_section -> line $line_no\n"; 
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
$section_line{"prologue"} = "1";

## generate the schema as a function
##
## Since we add N lines to the front, subtract the same N from
## $section_line{"schema"}, otherwise the compiler will generate off-by-N
## error messages.
##  (N=1 currently.)

my $schemasec = $sections{"schema"};
unless ($schemasec) {
    die "Section 'schema' missing, unable to proceed\n";
}

$schemasec = "schema() -> \n".$schemasec."\n\n";
$sections{"schema"} = $schemasec;
$section_line{"schema"} -= 1;   ## because we added 1 line to section above

########################################
## Phase 3: print the result

use POSIX;
my $datetime = strftime("%Y-%m-%d %H:%M:%S\n", localtime(time));

## We now have all the sections, print them
## in the order they appeared.

open (OUT, ">$outfile") or die "Unable to open $outfile: $!";
print OUT 
    "%% -*- Erlang -*-\n".
    "%% Generated: $datetime \n";
foreach my $key (@section_list) {
    my $sec_line = $section_line{$key} || die "Section line not found for '$key'";
    print OUT "%% SECTION ".$key."\n";
    print OUT "-file(\"$file\", $sec_line).\n";
    print OUT $sections{$key};
}
print OUT "%% END\n";
close OUT;

__END__
