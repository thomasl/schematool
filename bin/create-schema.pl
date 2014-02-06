#!/usr/bin/perl
#
# Wrapper to invoke erlang properly

use Getopt::Long;
use File::Basename qw(basename fileparse);
use Cwd qw(abs_path);
use strict;
use warnings;

my ($scriptfile, $SCHEMATOOL_BIN) = fileparse(abs_path($0));
my $SCHEMAMOD;
my $NODE;
my $ebin;

GetOptions(
    'node=s' => \$NODE,
    'ebin=s' => \$ebin,
    'schema=s' => \$SCHEMAMOD
    );

unless ($NODE) {
    print STDERR "No node given, assuming --node 'test'\n";
    $NODE='test';
}

unless ($SCHEMAMOD) {
    print STDERR "No schema given, use --schema modulename\n";
    exit(1);
}

my $pa_ebin = "";
if ($ebin) {
    $pa_ebin = "-pa $ebin";
}

my $erleval = "case schematool:create_schema($SCHEMAMOD) of ok -> init:stop(0); _ -> init:stop(1) end";

my $cmd = 
    "erl -noshell ".
    "-pa ../ebin ".
    "-pa $SCHEMATOOL_BIN/../ebin ".
    "$pa_ebin".
    "-name $NODE ".
    "-eval \"$erleval\"".
    "";
print STDERR $cmd."\n";
system($cmd);
