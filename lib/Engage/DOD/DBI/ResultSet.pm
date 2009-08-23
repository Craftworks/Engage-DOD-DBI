package Engage::DOD::DBI::ResultSet;

use Moose;
with 'Engage::DOD::Role::ResultSet';

has 'sth' => (
    is  => 'ro',
    isa => 'DBI::st',
    required => 1,
);

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
    wantarray ? %{ $self->sth->fetchrow_hashref } : $self->sth->fetchrow_hashref;
}

sub all_array {
    my $self = shift;
    return $self->sth->fetchall_arrayref;
}

sub all_hash {
    my $self = shift;
    return $self->sth->fetchall_arrayref(+{});
}

1;
