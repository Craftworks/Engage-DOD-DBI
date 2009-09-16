package Engage::DOD::DBI;

use Moose;
use DBI;
use SQL::Abstract::Limit;
use Data::Dumper;

extends 'Engage::DOD';
with 'Engage::DOD::Role::Driver';

our $VERSION = '0.01';

has 'connections' => (
    is  => 'ro',
    isa => 'HashRef[DBI::db]',
    default => sub { {} },
    lazy => 1,
);

has 'root_class' => (
    is  => 'ro',
    isa => 'Str',
    default => sub {
        shift->config->{'root_class'} || 'Engage::DOD::DBIx';
    },
    lazy => 1,
);

has 'sql_maker' => (
    is  => 'ro',
    isa => 'SQL::Abstract',
    default => sub {
        SQL::Abstract::Limit->new( limit_dialect => shift->dbh('R') );
    },
    lazy => 1,
);

has 'table_info' => (
    is  => 'rw',
    isa => 'HashRef',
    default => sub { {} },
);

no Moose;

__PACKAGE__->meta->make_immutable;

sub BUILD {
    my $self = shift;

    Class::MOP::Class->create(
        'Engage::DOD::DBIx::st', methods => { 'log' => sub { $self->log }  }
    );
}

sub dbh {
    my ( $self, $datasource ) = @_;

    confess q{Usage: $dod->('DBI')->dbh('DataSource')} unless defined $datasource;

    confess qq{Unknown datasource "$datasource"}
        if ( !exists $self->config->{'datasources'}{$datasource} );

    if ( !exists $self->connections->{$datasource} ) {
        $self->connections->{$datasource} = $self->_connect( $datasource );
    }

    if ( !$self->connections->{$datasource}->ping ) {
        $self->log->warn(q/The connection had already closed. Trying to reconnect./);
        $self->connections->{$datasource} = $self->_connect( $datasource );
    }

    return $self->connections->{$datasource};
}

sub _connect {
    my ( $self, $datasource ) = @_;

    my $connect_info = $self->config->{'datasources'}{$datasource};
    $connect_info->[3]{'RootClass'} ||= $self->root_class;
    return DBI->connect(@$connect_info);
}

sub _primary_key {
    my ( $self, $table, $schema, $catalog ) = @_;

    if ( !exists $self->table_info->{$table}{'primary_key'} ) {
        my @primary_key = $self->dbh('R')->primary_key( $catalog, $schema, $table );
        $self->table_info->{$table}{'primary_key'} = \@primary_key;
    }

    return $self->table_info->{$table}{'primary_key'};
}

sub _execute {
    my ( $self, $datasource, $sql, @bind_params ) = @_;
    my $sth = $self->dbh($datasource)->prepare($sql);
    $sth->execute(@bind_params) or confess 'DBI::st aborted';
    return $self->_resultset( $sth );
}

sub _resultset {
    my ( $self, $sth ) = @_;
    $self->result_class->new( sth => $sth );
}

sub query {
    my ( $self, $sql, @bind_params ) = @_;
    my $datasource = $sql =~ /^\s*(?:SHOW|SELECT)\b/io ? 'R' : 'W';
    return $self->_execute( $datasource, $sql, @bind_params );
}

sub create {
    my ( $self, $table, $row ) = @_;
    my ( $sql, @bind_params ) = $self->sql_maker->insert( $table, $row );
    return $self->_execute( 'W', $sql, @bind_params );
}

sub read {
    my ( $self, $table, $fields, $where, $order, @rest ) = @_;

    my ($sql, @bind_params) = $self->sql_maker->select(
        $table, $fields, $where, $order, @rest
    );

    return $self->_execute( 'R', $sql, @bind_params );
}

sub update {
    my ( $self, $table, $row ) = @_;

    my $set = { %$row };
    my $where = $self->_make_where_from_primary_key( $table, $set );
    my ( $sql, @bind_params ) = $self->sql_maker->update(
        $table, $set, $where
    );

    return $self->_execute( 'W', $sql, @bind_params );
}

sub delete {
    my ( $self, $table, $where ) = @_;

    my ( $sql, @bind_params ) = $self->sql_maker->delete(
        $table, $where
    );

    return $self->_execute( 'W', $sql, @bind_params );
}

sub create_bulk {
    my ( $self, $table, $rows ) = @_;

    return unless @$rows;

    my ($sql) = $self->sql_maker->insert( $table, $rows->[0] );
    my $sth = $self->dbh('W')->prepare($sql);

    my ( $columns_phrase ) = $sql =~ /\((.+?)\) VALUES/o;
    my @columns = $columns_phrase =~ /\b(\w+?)\b/go;

    my $index = 1;
    for my $column ( @columns ) {
        my @data = map $_->{$column}, @$rows;
        $sth->bind_param_array( $index++, \@data );
    }

    my $rv = $sth->execute_array({ ArrayTupleStatus => \my @tuple_status });
    $rv;
}

sub update_bulk {
    my ( $self, $table, $rows ) = @_;

    return unless @$rows;

    my $row   = +{ %{ $rows->[0] } };
    my $where = $self->_make_where_from_primary_key( $table, $row );

    my ( $sql, @bind_params ) = $self->sql_maker->update(
        $table, $row, $where
    );
    my $sth = $self->dbh('W')->prepare($sql);

    my ( $set_phrase, $where_phrase ) = $sql =~ /SET(.+?)WHERE(.+)/o;
    my @set_columns   = $set_phrase   =~ /\b(\w+?)\b/go;
    my @where_columns = $where_phrase =~ /\b(\w+?)\b/go;

    my $index = 1;
    for my $column ( @set_columns, @where_columns ) {
        my @data = map $_->{$column}, @$rows;
        $sth->bind_param_array( $index++, \@data );
    }

    my $rv = $sth->execute_array({ ArrayTupleStatus => \my @tuple_status });
    $rv;
}

sub read_or_create {
    my ( $self, $table, $row ) = @_;

    return $self->read_or_create_bulk( $table, $row ) if ( ref $row eq 'ARRAY' );

    my $primary_key = $self->_primary_key( $table );
    my $where = $self->_make_where_from_primary_key( $table, {%$row} );

    my ($sql, @bind_params) = $self->sql_maker->select(
        $table, $primary_key, $where
    );
    my $rs = $self->_execute( 'R', $sql, @bind_params );

    return $rs->next
        ? $rs->rows
        : $self->create( $table, $row )->rows;
}

sub read_or_create_bulk {
    my ( $self, $table, $rows ) = @_;

    return unless @$rows;

    # primary key check
    my $primary_key = $self->_primary_key( $table );

    my $columns = join q{, }, @$primary_key;
    my $values  = join q{, }, (sprintf '(%s)', join q{, }, ('?') x @$primary_key) x @$rows;
    my $sql = sprintf 'SELECT %s FROM %s WHERE (%s) IN (%s)', $columns, $table, $columns, $values;
    my @bind_params = map @$_{@$primary_key}, @$rows;

    # already exists
    my $rs = $self->_execute( 'R', $sql, @bind_params );

    my %exists;
    $exists{ join '\t', @$_ } = 1 for @{ $rs->all_array };

    # no rows to create
    return @$rows if ( $rs->rows == @$rows );

    # split data
    my @create_data;
    for my $row ( @$rows ) {
        my $key = join '\t', @$row{@$primary_key};
        push @create_data, $row if ( !$exists{ $key } );
    }

    return $self->create_bulk( $table, \@create_data );
}

sub update_or_create {
    my ( $self, $table, $row ) = @_;

    return $self->update_or_create_bulk( $table, $row ) if ( ref $row eq 'ARRAY' );

    my $primary_key = $self->_primary_key( $table );
    my $where = $self->_make_where_from_primary_key( $table, {%$row} );

    my ($sql, @bind_params) = $self->sql_maker->select(
        $table, $primary_key, $where
    );
    my $rs = $self->_execute( 'R', $sql, @bind_params );

    return $rs->next
        ? $self->update( $table, $row )
        : $self->create( $table, $row );
}

sub update_or_create_bulk {
    my ( $self, $table, $rows ) = @_;

    # primary key check
    my $primary_key = $self->_primary_key( $table );

    my $columns = join q{, }, @$primary_key;
    my $values  = join q{, }, (sprintf '(%s)', join q{, }, ('?') x @$primary_key) x @$rows;
    my $sql = sprintf 'SELECT %s FROM %s WHERE (%s) IN (%s)', $columns, $table, $columns, $values;
    my @bind_params = map @$_{@$primary_key}, @$rows;

    # already exists
    my $rs = $self->_execute( 'R', $sql, @bind_params );

    my %exists;
    $exists{ join '\t', @$_ } = 1 for @{ $rs->all_array };

    # split data
    my @create_data;
    my @update_data;
    for my $row ( @$rows ) {
        my $key = join '\t', @$row{@$primary_key};
        $exists{ $key }
            ? push @update_data, $row
            : push @create_data, $row
    }

    my $rv_create = $self->create_bulk( $table, \@create_data ) || 0;
    my $rv_update = $self->update_bulk( $table, \@update_data ) || 0;

    return $rv_create + $rv_update;
}

sub _make_where_from_primary_key {
    my ( $self, $table, $row, $where ) = @_;

    $where ||= {};
    my $primary_key = $self->_primary_key( $table );
    for (@$primary_key) {
        if ( !exists $row->{$_} ) {
            local $Data::Dumper::Indent = 0;
            local $Data::Dumper::Terse  = 1;
            confess 'primary key not found in data ' . Dumper $row;
        }
        $where->{$_} = delete $row->{$_};
    }

    return $where;
}

package Engage::DOD::DBIx;
use base 'DBI';

package Engage::DOD::DBIx::db;
use base 'DBI::db';

package Engage::DOD::DBIx::st;
use strict;
use warnings;
use base 'DBI::st';
use Time::HiRes;

sub execute {
    my ( $self, @bind_params ) = @_;

    local $Log::Dispatch::Config::CallerDepth = 1;
    $self->log->info(sprintf '%s: %s', $self->{'Statement'}, join(q{, }, @bind_params));

    local $self->{'PrintError'} = 0;
    my $time   = Time::HiRes::time;
    my $rv     = $self->SUPER::execute( @bind_params );
    my $elapse = Time::HiRes::time - $time || 0.000001;
    my $tps    = 1 / $elapse;

    if ( $rv ) {
        $self->log->info(sprintf 'Query took %.6fs (%.3f/s)', $elapse, $tps);
    }
    else {
        $self->log->error('execute failed: ' . $self->errstr);
    }

    return $rv;
}

1;

=head1 NAME

Engage::DOD::DBI - The great new Engage::DOD::DBI!

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Engage::DOD::DBI;

    my $foo = Engage::DOD::DBI->new();
    ...

=head1 METHODS

=cut

=head1 AUTHOR

Craftworks, C<< <craftwork at cpan org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-engage-dod-dbi at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Engage-DOD-DBI>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Engage::DOD::DBI

You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Engage-DOD-DBI>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Engage-DOD-DBI>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Engage-DOD-DBI>

=item * Search CPAN

L<http://search.cpan.org/dist/Engage-DOD-DBI/>

=back

=head1 ACKNOWLEDGEMENTS

=head1 COPYRIGHT & LICENSE

Copyright 2009 Craftworks.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.

=cut
