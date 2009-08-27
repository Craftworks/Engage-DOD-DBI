package Engage::DAO::Role::DBI;

use Moose::Role;

our $VERSION = '0.01';

no Moose::Role;

sub dbh {
    shift->dod('DBI')->dbh(shift);
}

sub create {
    my $self = shift;
    $self->dod('DBI')->create( $self->data_name, @_ );
}

sub read {
    my $self = shift;
    $self->dod('DBI')->read( $self->data_name, @_ );
}

sub update {
    my $self = shift;
    $self->dod('DBI')->update( $self->data_name, @_ );
}

sub delete {
    my $self = shift;
    $self->dod('DBI')->delete( $self->data_name, @_ );
}

sub read_or_create {
    my $self = shift;
    $self->dod('DBI')->read_or_create( $self->data_name, @_ );
}

sub update_or_create {
    my $self = shift;
    $self->dod('DBI')->update_or_create( $self->data_name, @_ );
}

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
