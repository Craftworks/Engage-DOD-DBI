package Engage::DOD::DBI::ResultSet;

use Moose;
with 'Engage::DOD::Role::ResultSet';

has 'sth' => (
    is  => 'ro',
    isa => 'DBI::st',
    required => 1,
);

no Moose;

__PACKAGE__->meta->make_immutable;

sub rows {
    my $self = shift;
    my $rows = $self->sth->rows;
    ($rows == 0) ? '0E0' : $rows;
}

sub next_array {
    my $self = shift;
    wantarray ? $self->sth->fetchrow_array : $self->sth->fetchrow_arrayref;
}

sub next_hash {
    my $self = shift;
    my $row = $self->sth->fetchrow_hashref;
    $row ? ( wantarray ? %$row : $row ) : undef;
}

sub all_array {
    my ( $self, $index ) = @_;
    if ( defined $index ) {
        return [ map $_->[$index], @{$self->sth->fetchall_arrayref} ];
    }
    else {
        return $self->sth->fetchall_arrayref;
    }
}

sub all_hash {
    my $self = shift;
    return $self->sth->fetchall_arrayref(+{});
}

1;
