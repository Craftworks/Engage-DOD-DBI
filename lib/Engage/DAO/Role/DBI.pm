package Engage::DAO::Role::DBI;

use Moose::Role;
use SQL::Abstract::Limit;
use Data::Dumper;

our $VERSION = '0.01';

with 'Engage::DAO::Role';

has 'sql_maker' => (
    is  => 'ro',
    isa => 'SQL::Abstract',
    default => sub {
        SQL::Abstract::Limit->new( limit_dialect => shift->dbh('R') );
    },
    lazy => 1,
);

has 'primary_key' => (
    is  => 'ro',
    default => sub {
        my $self = shift;
        my $catalog;
        my $schema;
        my $table = $self->data_name;
        [ $self->dbh('R')->primary_key( $catalog, $schema, $table ) ];
    },
    lazy => 1,
);

no Moose::Role;

sub dbh {
    shift->dod('DBI')->dbh(shift);
}

sub resultset {
    my ( $self, $sth ) = @_;
    $self->_build_resultset( 'DBI', $sth );
}

sub execute {
    my ( $self, $datasource, $sql, @bind_params ) = @_;
    my $sth = $self->dbh($datasource)->prepare($sql);
    $sth->execute(@bind_params) or confess 'DBI::st aborted';
    return $self->resultset( $sth );
}

sub create {}

sub search {
    my ( $self, $fields, $where, $order, @rest ) = @_;

    my ($sql, @bind_params) = $self->sql_maker->select(
        $self->data_name, $fields, $where, $order, @rest
    );

    return $self->execute( 'R', $sql, @bind_params );
}

sub update {
    my ( $self, $row ) = @_;

    my %where;

    my $primary_key = $self->primary_key;
    for (@$primary_key) {
        if ( !exists $row->{$_} ) {
            local $Data::Dumper::Indent = 0;
            local $Data::Dumper::Terse  = 1;
            confess 'primary key not found in data ' . Dumper $row
        }
        $where{$_} = delete $row->{$_};
    }

    my ($sql, @bind_params) = $self->sql_maker->update(
        $self->data_name, $row, \%where
    );

    return $self->execute( 'R', $sql, @bind_params );
}

sub delete {}

1;

=head1 NAME

Engage::DAO::Role::DBI - The great new Engage::DAO::Role::DBI!

=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use Engage::DAO::Role::DBI;

    my $foo = Engage::DAO::Role::DBI->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 AUTHOR

Craftworks, C<< <craftwork at cpan org> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-engage-dod-dbi at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Engage-DOD-DBI>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Engage::DAO::Role::DBI

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
