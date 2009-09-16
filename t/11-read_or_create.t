use strict;
use warnings;
use FindBin;
use Test::More tests => 7;
use Test::mysqld;
use Data::Dumper;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my $mysqld = Test::mysqld->new(
    my_cnf => { 'skip-networking' => '' }
) or plan skip_all => $Test::mysqld::errstr;
my $socket = $mysqld->my_cnf->{'socket'};

system('mysql', "--socket=$socket", '-u', 'root', '-e', 'CREATE DATABASE test');

my %sql = (
    create_table => q{CREATE TABLE test ( id INT, chr VARCHAR(16), PRIMARY KEY (id, chr) )},
    drop_table   => q{DROP TABLE test},
);

my $dod = new_ok( 'Engage::DOD::DBI' );

$dod->config->{'datasources'}{'W'}[0] = "dbi:mysql:test;mysql_socket=$socket";
$dod->config->{'datasources'}{'R'}[0] = "dbi:mysql:test;mysql_socket=$socket";

my $dbh = $dod->dbh('W');

$dbh->do($sql{'drop_table'});
$dbh->do($sql{'create_table'}); 

#=============================================================================
# single
#=============================================================================
{
    my $data = { 'id'  => 1, 'chr' => 'a' };

    cmp_ok( $dod->read_or_create( 'test', $data ), '==', 1, 'read_or_create affects 1 tuple' );
    is_deeply( scalar $dod->read( 'test', ['*'], { id => 1 })->next, $data, 'create success' );
    cmp_ok( $dod->read_or_create( 'test', $data ), '==', 1, 'read_or_create affects 1 tuple' );
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
    cmp_ok( $dod->read_or_create_bulk( 'test', $data ), '==', 4, 'read_or_create_bulk affects 5 tuples' );
    cmp_ok( $dod->read_or_create_bulk( 'test', $data ), '==', 5, 'read_or_create_bulk affects 5 tuples' );
}

$dbh->do($sql{'drop_table'});

