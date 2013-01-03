package Dragon::Scales::Util;

use strict;
use warnings;

use RRDs;
use AnyEvent::Worker;
use parent 'Exporter';

our @EXPORT = qw(rrd_fetch rrd_create);

our $WORKER = AnyEvent::Worker->new(sub {
  my ($meth, $file, $named, @extra) = @_;
  my @named = map {("--$_", $named->{$_}); } keys %$named;
  no strict "refs";
  *{__PACKAGE__ . "::_$meth"}->($file, @named, @extra);
});

sub _fetch {
  my $file = shift;
  my ($start, $step, $names, $data) = RRDs::fetch $file, "AVERAGE", @_;
  [ map { [ $start + $step * $_ => $data->[$_][0] ] } 0 .. @$data - 1 ];
}

sub _create {
  my $file = shift;
  RRDs::create $file, @_;
}

sub rrd_fetch  {
  my $cb = pop;
  $WORKER->do(fetch => @_, sub { $cb->($_[1])})
}
sub rrd_create {
  my $cb = pop;
  $WORKER->do(create => @_, sub { $cb->($_[1])})
}

1;
