#!/usr/bin/perl
#
# Toplevel
#
# UNFINISHED
# - just generates+compiles schema module
# - module probably should have VERSIONED name
#   * foundation of schema diffs (in erlang ctx)
# - escape/quote options values?
# - option 'einc' is not pretty

use Getopt::Long;
use File::Basename qw(basename);
use strict;
use warnings;

## schemamod must be in path, ie this
## directory must be in your path

my $SCHEMAMOD_PL = "schemamod.pl";

## Mangle options

my $schemafile;
my $schemamod;
my $outf;
my $etmp;
my $ebin;
my $einc;
my $epath;

GetOptions(
    'schemafile=s' => \$schemafile,
    'schemamod' => \$schemamod,
    'outfile=s' => \$outf,
    'etmp=s' => \$etmp,
    'ebin=s' => \$ebin,
    'einc=s' => \$einc,
    'epath=s' => \$epath
    );

sub usage() {
    print STDERR 
	"usage: schematool [options]\n".
	" --schemafile file.schema\n".
	" [--schemamod modname]\n".
	" --outfile file.erl\n".
	" [--etmp tmpdir]\n".
	" [--ebin beam output dir]\n".
	" [--einc erlc include dir]\n".
	" [--epath erlc extra path]\n".
	"";
}

unless ($schemafile) {
    die "No schema file given (--schemafile file.schema)";
}

unless (-f $schemafile) {
    die "Schema file not found: $!";
}

unless ($schemamod) {
    my $base = basename($schemafile, ".schema");
    $schemamod = $base."_schema";
}

# Should use /usr/tmp on linux
# Not sure about correct windows location
# OSX: no /usr/tmp on my mac, so I'm using /tmp

unless ($etmp) {
    $etmp = "/tmp";
}

unless ($ebin) {
    $ebin = ".";
}

unless ($einc) {
    $einc = ".";
}

unless ($epath) {
    $epath = ".";
}

unless ($outf) {
    $outf = $etmp."/".$schemamod.".erl";
}

# Get rid of this
print STDERR "Note: No escaping of arguments done; spaces etc may cause trouble\n";

## Body of script
##
## UNFINISHED
## - Not sure why system() pretends to fail, wrong return code? what?
## - Seems to work at the moment

my $cmd;
$cmd = "perl $SCHEMAMOD_PL --schemafile $schemafile --outfile $outf --module $schemamod --force";
print STDERR "$cmd\n";
system($cmd) 
#    or die "schemamod failed: $!"
;

$cmd = "erlc -I $einc -pa $epath -o $ebin $outf";
print STDERR "$cmd\n";
system($cmd) 
#    or die "erlc failed: $!"
;

## At this point, you can run the rest in
## erlang, as
##   erl -pa $ebin ...
##
## In erlang, call $schemamod:schema() to get the schema definition
