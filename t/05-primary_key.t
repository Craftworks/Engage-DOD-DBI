use strict;
use warnings;
use FindBin;
use Test::More tests => 4;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my $dod = new_ok( 'Engage::DOD::DBI' );

my %sql = (
    single => q{CREATE TABLE test_single ( key INT, PRIMARY KEY (key) )},
    multi  => q{CREATE TABLE test_multi  ( key1 INT, key2 TEXT, PRIMARY KEY (key1, key2) )},
);

my $dbh = $dod->dbh('W');

$dbh->do($sql{'single'}); 
$dbh->do($sql{'multi'}); 

is_deeply( $dod->_primary_key( 'test_single' ), [qw/key/], 'single primary key' );
is_deeply( $dod->_primary_key( 'test_multi' ), [qw/key1 key2/], 'multi primary key' );

