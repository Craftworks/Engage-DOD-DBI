use strict;
use warnings;
use FindBin;
use Test::More tests => 4;
use Data::Dumper;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my %sql = (
    create_table => q{CREATE TABLE test ( id INT, PRIMARY KEY (id) )},
    drop_table   => q{DROP TABLE test},
);

my $data = [
    { 'id'  => 1 },
    { 'id'  => 2 },
    { 'id'  => 3 },
    { 'id'  => 4 },
    { 'id'  => 5 },
];

my $dod = new_ok( 'Engage::DOD::DBI' );

my $dbh = $dod->dbh('W');

$dbh->do($sql{'drop_table'});
$dbh->do($sql{'create_table'}); 

cmp_ok( $dod->create_bulk( 'test', $data ), '==', 5, 'create_bulk affects 5 tuples' );
is_deeply( $dod->read( 'test', ['id'] )->all, $data, 'create_bulk success' );

