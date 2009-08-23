package Engage::DOD::DBI;

use Moose;
use DBI;
extends 'Engage::DOD';

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
        $self->connections->{$datasource} = $self->connect( $datasource );
    }

    if ( !$self->connections->{$datasource}->ping ) {
        $self->log->warn(q/The connection had already closed. Trying to reconnect./);
        $self->connections->{$datasource} = $self->connect( $datasource );
    }

    return $self->connections->{$datasource};
}

sub connect {
    my ( $self, $datasource ) = @_;

    my $connect_info = $self->config->{'datasources'}{$datasource};
    $connect_info->[3]{'RootClass'} ||= $self->root_class;
    return DBI->connect(@$connect_info);
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
    $self->log->debug(sprintf '%s: %s', $self->{'Statement'}, join(q{, }, @bind_params));

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
