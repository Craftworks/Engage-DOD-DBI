use strict;
use warnings;
use FindBin;
use Test::More tests => 7;
use Test::Exception;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my $dod = new_ok( 'Engage::DOD::DBI' );

dies_ok { $dod->dbh } 'not specify datasource';
dies_ok { $dod->dbh('X') } 'specify unknown datasource';

isa_ok( $dod->dbh('R'), 'DBI::db' );

ok( $dod->dbh('R')->ping, 'ping' );

$dod->dbh('R')->disconnect;
ok( $dod->dbh('R')->ping, 'ping after disconnect' );

