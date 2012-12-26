package AnyEvent::rrdcache::Test;

use parent 'Exporter';

use AnyEvent::rrdcache;
use AnyEvent::rrdcached;
use Cwd;

our @EXPORT = qw/test_rrdcache/;
my @servers;

sub test_rrdcache {
  my $cb = shift;
  my $dir = Cwd::abs_path "t/cached";
  mkdir "$dir/$_" for "", "rrds", "journal";

  my $server = AnyEvent::rrdcached->new($dir);
  $server->spawn->recv;

  my $client = AnyEvent::rrdcache->new(
    host => "unix/",
    port => $server->sock
  );

  push @servers, $server;
  $cb->($client);
}

1;
