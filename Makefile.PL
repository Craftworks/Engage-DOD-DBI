use inc::Module::Install;

name     'Engage-DOD-DBI';
all_from 'lib/Engage/DOD/DBI.pm';
author   q{Craftworks <craftwork at cpan org>};
license  'perl';

build_requires 'Test::More';

requires('Moose');
requires('DBI');

auto_install;

WriteAll;

