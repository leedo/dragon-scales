package Dragon::Scales;

use v5.14;

use AnyEvent;
use AnyEvent::Util;
use AnyEvent::rrdcache;
use Dragon::Scales::Worker;
use Dragon::Scales::Request;

sub new {
  my ($class, %args) = @_;
  for (qw{port host dir}) {
    die "$_ is required" unless defined $args{$_};
  }

  my $client = AnyEvent::rrdcache->new(
    host => $args{host},
    port => $args{port},
  );

  my $self = bless {
    client => $client,
    buffer => {},
    %args
  }, $class;

  $self->{t} = AE::timer 60, 60, sub { $self->flush };

  return $self;
}

sub flush {
  my $self = shift;
  my $time = AE::time;
  my $c = $self->{client};

  for my $id (keys %{$self->{buffer}}) {
    for my $stat (keys %{$self->{buffer}{$id}}) {
      my $file = "$self->{dir}/$id/$stat.rrd";
      my $value = $self->{$id}{$stat};

      if (!-e $file) {
        $self->create($id, $stat, sub {
          $c->update($file, "$time:$value")
        });
        next;
      }

      $c->update($file, "$time:$value", sub {});
    }
    delete $self->{buffer}{$id};
  }
}

sub handle_req {
  my ($self, $req) = @_;

  my ($id, $action) = split "/", substr $req->path, 1;
  return $req->error("missing id") unless defined $id;

  do {
    given ($action) {
      when ("incr") { $self->incr($id, $req) }
      when ("stat") { $self->fetch($id, $req) }
      default { $req->respond("invalid action") }
    }
  };
}

sub incr {
  my ($self, $id, $req) = @_;
  my @stats = $req->parameters->get_all("stats");
  $self->{buffer}{$id}{$_}++ for @stats;
  $req->respond("ok");
}

sub create {
  my ($self, $id, $stat, $cb) = @_;
  my $file = "$self->{dir}/$id/$stat.rrd";

  die "file already exists" if -e $file;
  mkdir "$self->{dir}/$id" unless -d "$self->{dir}/$id";

  rrd_create $file, {
      start => time,
      step  => 60,
    },
    "DS:$stat:GAUGE:120:U:U",
    "RRA:AVERAGE:0.5:1:120", # one minute interval for 2 hours
    "RRA:AVERAGE:0.5:5:576", # five minute interval for two days
    "RRA:AVERAGE:0.5:30:1400", # thirty minute interval for a month
    "RRA:AVERAGE:0.5:720:704", # 12 hour interval for a year
    $cb;
}

sub fetch {
  my ($self, $id, $req) = @_;
  my $stat = $req->parameters->{stat};
  my $file = "$self->{dir}/$id/$stat.rrd";
  my $time = time;

  rrd_fetch $file, {
      start => $time - 3600,
      end   => $time,
      daemon => "unix:$self->{port}",
    },
    sub {
      my $samples = shift;
      return $req->error("unable to fetch") unless $samples;
      $req->respond([map { [$_->[0], $_->[1] || 0] } @$samples]);
    };
}

sub to_app {
  my $self = shift;
  sub {
    my $env = shift;
    sub {
      my $req = Dragon::Scales::Request->new($env, shift);
      $self->handle_req($req);
    };
  }
}

1;
