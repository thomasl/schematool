#!/usr/bin/perl
#
# Return the current hostname for your
# operating system.
#
# UNFINISHED
# - OSX: returns things like foo.local which we chop off, should we
#   keep the .local?

use Config;

my $os = $Config{osname};
if ($os == "darwin") {
    my @res = `hostname -f`;
    my $long_host = @res[0];
    $long_host =~ s/.local$//;
    print STDOUT $long_host;
} elsif ($os == "linux") {
    ## untested, could also be 'hostname --long'
    ## - should return the FQDN
    system("hostname -f");
} else {
    ## FreeBSD should work
    ## Windows, cough
    print STDERR "OS not supported, please fix this\n";
    exit(1);
}

