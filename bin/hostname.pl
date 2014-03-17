#!/usr/bin/perl
#
# Return the current hostname for your
# operating system.

use Config;

my $os = $Config{osname};
if ($os == "darwin") {
    system("hostname -f");
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

