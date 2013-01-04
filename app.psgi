#!/usr/bin/env perl

use v5.14;

use Dragon::Scales;
use AnyEvent::rrdcache;
use AnyEvent::rrdcached;
use Plack::Builder;
use Cwd;

my $dir = Cwd::abs_path($ENV{DRAGONSCALES_PATH} || "./cached");
mkdir "$dir/$_" for qw{rrds journal};

# need server to stick around, because it shuts
# down when it goes out of scope. better solution?
our $server = AnyEvent::rrdcached->new($dir);
$server->spawn->recv;

my $rrd = AnyEvent::rrdcache->new(
  host => $server->host,
  port => $server->port,
);

my $dragon = Dragon::Scales->new(
  dir  => $server->rrd_dir,
  cached => $rrd,
);

builder {
  mount "/" => $dragon->to_app;
};
