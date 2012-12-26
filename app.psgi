#!/usr/bin/env perl

use v5.14;

use Dragon::Scales;
use AnyEvent::rrdcache;
use AnyEvent::rrdcached;
use Plack::Builder;
use Cwd;

my $dir = Cwd::abs_path($ENV{DRAGONSCALES_PATH} || "./cached");
mkdir "$dir/rrds";

# need server to stick around, because it shuts
# down when it goes out of scope. better solution?
our $server = AnyEvent::rrdcached->new($dir);
$server->spawn->recv;

my $client = AnyEvent::rrdcache->new($server->dsn);
my $dragon = Dragon::Scales->new($client, "$dir/rrds");

builder {
  mount "/" => $dragon->to_app;
};