use strict;
use warnings;
use FindBin;
use Test::More tests => 9;
use Data::Dumper;

use_ok 'Engage::DOD::DBI';

$ENV{'CONFIG_PATH'} = "$FindBin::Bin/conf";

my %sql = (
    create_table => q{CREATE TABLE test ( id INT, num INT, chr VARCHAR(32), PRIMARY KEY (id) )},
    drop_table   => q{DROP TABLE test},
);

my $data = {
    'id'  => 1,
    'num' => 999,
    'chr' => 'blah blah blah',
};

my $dod = new_ok( 'Engage::DOD::DBI' );

my $dbh = $dod->dbh('W');

$dbh->do($sql{'drop_table'});
$dbh->do($sql{'create_table'}); 

#=============================================================================
# create
#=============================================================================
{
    my $rs = $dod->create( 'test', $data );
    isa_ok( $rs, 'Engage::DOD::DBI::ResultSet', 'create success' );
    is( $rs->rows, 1, 'create affects one tuple' );
}

#=============================================================================
# read
#=============================================================================
{
    my $rs = $dod->read( 'test', [qw/id num chr/], {} );
    is_deeply( scalar $rs->next, $data, 'read returns inserted data' );
}

#=============================================================================
# update
#=============================================================================
{
    $data->{'chr'} = 'yadayadayada';
    my $rs = $dod->update( 'test', $data );
    is( $rs->rows, 1, 'update affects one tuple' );
    my $row = $dod->read( 'test', [qw/id num chr/], {} )->next;
    cmp_ok( $row->{'chr'}, 'eq', 'yadayadayada', 'update success' );
}

#=============================================================================
# delete
#=============================================================================
{
    my $rs = $dod->delete( 'test', { id => 1 } );
    is( $rs->rows, 1, 'delete affects one tuple' );
    $rs = $dod->read( 'test', [qw/id num chr/], {} );
    ok( !$rs->next, 'delete success' );
}

$dbh->do($sql{'drop_table'});

