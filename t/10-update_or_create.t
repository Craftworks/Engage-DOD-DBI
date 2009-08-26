use strict;
use warnings;
use FindBin;
use Test::More tests => 8;
use Data::Dumper;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my %sql = (
    create_table => q{CREATE TABLE test ( id INT, chr VARCHAR(16), PRIMARY KEY (id) )},
    drop_table   => q{DROP TABLE test},
);

my $dod = new_ok( 'Engage::DOD::DBI' );

my $dbh = $dod->dbh('W');

$dbh->do($sql{'drop_table'});
$dbh->do($sql{'create_table'}); 

#=============================================================================
# single
#=============================================================================
{
    my $data = { 'id'  => 1, 'chr' => 'a' };

    cmp_ok( $dod->update_or_create( 'test', $data )->rows, '==', 1, 'update_or_create affects 1 tuple' );
    is_deeply( scalar $dod->read( 'test', ['*'], { id => 1 })->next, $data, 'create success' );

    cmp_ok( $dod->update_or_create( 'test', $data )->rows, '==', 1, 'update_or_create affects 1 tuple' );
    is_deeply( scalar $dod->read( 'test', ['*'], { id => 1 })->next, $data, 'update success' );
}

#=============================================================================
# multi
#=============================================================================
{
    my $data = [
        { 'id'  => 1, 'chr' => 'a' },
        { 'id'  => 2, 'chr' => 'b' },
        { 'id'  => 3, 'chr' => 'c' },
        { 'id'  => 4, 'chr' => 'd' },
        { 'id'  => 5, 'chr' => 'e' },
    ];
    cmp_ok( $dod->update_or_create_bulk( 'test', $data ), '==', 5, 'update_or_create_bulk affects 5 tuples' );
    cmp_ok( $dod->update_or_create_bulk( 'test', $data ), '==', 5, 'update_or_create_bulk affects 5 tuples' );
}


$dbh->do($sql{'drop_table'});
