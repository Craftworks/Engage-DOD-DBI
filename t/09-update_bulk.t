use strict;
use warnings;
use FindBin;
use Test::More tests => 4;
use Data::Dumper;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my %sql = (
    create_table => q{CREATE TABLE test ( id INT, chr VARCHAR(16), PRIMARY KEY (id) )},
    drop_table   => q{DROP TABLE test},
);

my $data = [
    { 'id'  => 1, 'chr' => 'a' },
    { 'id'  => 2, 'chr' => 'b' },
    { 'id'  => 3, 'chr' => 'c' },
    { 'id'  => 4, 'chr' => 'd' },
    { 'id'  => 5, 'chr' => 'e' },
];

my $dod = new_ok( 'Engage::DOD::DBI' );

my $dbh = $dod->dbh('W');

$dbh->do($sql{'drop_table'});
$dbh->do($sql{'create_table'}); 
$dod->create_bulk( 'test', $data );

$data->[0]{chr} = 'z';
$data->[2]{chr} = 'x';
$data->[4]{chr} = 'y';

cmp_ok( $dod->update_bulk( 'test', $data ), '==', 5, 'create_bulk affects 5 tuples' );
is_deeply( $dod->read( 'test', [qw/id chr/] )->all, $data, 'update_bulk success' );
