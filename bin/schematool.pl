#!/usr/bin/perl
#
# Toplevel
#
# UNFINISHED
# - just generates+compiles schema module
# - escape/quote options values? e.g., secure paths with weird chars
# - option 'einc' is not pretty

use Getopt::Long;
use File::Basename qw(basename fileparse);
use Cwd qw(abs_path);
use strict;
use warnings;

## (There has to be a better way to do this. Motivation: the
## schemamod.pl script is found in the same dir as this script; we
## need to run it even when we are in some other dir and it's not in
## the path.)

my ($scriptfile, $SCHEMATOOL_BIN) = fileparse(abs_path($0));
my $SCHEMAMOD_PL = $SCHEMATOOL_BIN."schemamod.pl";

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
system($cmd);

my @VSNFILE = `perl $SCHEMAMOD_PL --schemafile $schemafile --vsnfile --outfile $outf`;
my $vsnfile = $VSNFILE[0];
print STDERR "vsn file is $vsnfile\n";

$cmd = "erlc -I $einc -pa $epath -o $ebin $outf";
print STDERR "$cmd\n";
system($cmd) ;

$cmd = "erlc -I $einc -pa $epath -o $ebin $vsnfile";
print STDERR "$cmd\n";
system($cmd) ;

## At this point, you can run the rest in
## erlang, as
##   erl -pa $ebin ...
##
## In erlang, call $schemamod:schema() to get the schema definition
