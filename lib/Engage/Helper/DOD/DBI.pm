package Engage::Helper::DOD::DBI;

=head1 NAME

Engage::Helper::DOD::DBI- Helper for Engage::DOD::DBI

=head1 SYNOPSIS

  script/create.pl DOD::DBI

=head1 DESCRIPTION

Helper for Engage::DOD::DBI

=head1 METHODS

=head2 mk_stuff

This is called by L<Catalyst::Helper> with the commandline args to generate the
files.

=head1 SEE ALSO

L<Catalyst::Helper>, L<Catalyst>,

=head1 AUTHOR

Craftworks, C<< <craftwork at cpan org> >>

=head1 LICENSE

This library is free software, you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

use Moose;

has helper => ( is => 'ro', isa => 'Engage::Helper', required => 1 );

sub mk_stuff {
    my ( $package, $helper ) = @_;

    my $self = $package->new(
        helper => $helper,
    );

    $self->_parse_args;
    $self->_mk_files;
}

sub _parse_args {
    my $self = shift;
    my $helper = $self->{helper};

    $self->{dod} = File::Spec->catfile( $helper->{base}, 'lib', $helper->{app}, 'DOD' );
    $self->{dbi} = File::Spec->catfile( $self->{dod}, 'DBI' );
    $self->{resultset} = File::Spec->catfile( $self->{dbi}, 'ResultSet' );
}

sub _mk_files {
    my $self = shift;
    my $helper = $self->{helper};

    $helper->render_file( 'dbi', $self->{dbi} . '.pm', $self->{helper} );
    $helper->mk_dir( $self->{dbi} );
    $helper->render_file( 'resultset', $self->{resultset} . '.pm', $self->{helper} );
}

1;

__DATA__

__dbi__
package [% app %]::DOD::DBI;

=head1 NAME

[% app %]::DOD::DBI - Engage DOD DBI

=head1 DESCRIPTION

Engage DOD.

=cut

use Moose;

extends 'Engage::DOD::DBI';

no Moose;

__PACKAGE__->meta->make_immutable;

=head1 SEE ALSO

L<Engege>, L<Engage::DOD::DBI>

=head1 AUTHOR

[%author%]

=head1 LICENSE

This library is free software. You can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
__resultset__
package [% app %]::DOD::DBI::ResultSet;

=head1 NAME

[% app %]::DOD::DBI::ResultSet - Engage DOD DBI Result Set

=head1 DESCRIPTION

Engage DOD.

=cut

use Moose;

extends 'Engage::DOD::DBI::ResultSet';

no Moose;

__PACKAGE__->meta->make_immutable;

=head1 SEE ALSO

L<Engege>, L<Engage::DOD::DBI>, L<Engage::DOD::DBI::ResultSet>

=head1 AUTHOR

[%author%]

=head1 LICENSE

This library is free software. You can redistribute it and/or modify
it under the same terms as Perl itself.

=cut

1;
