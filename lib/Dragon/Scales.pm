package Dragon::Scales;

use v5.14;

use AnyEvent;
use AnyEvent::Util;
use Dragon::Scales::Util;
use Dragon::Scales::Request;

sub new {
  my ($class, %args) = @_;
  for (qw{dir cached redis}) {
    die "$_ is required" unless defined $args{$_};
  }

  my $self = bless { int => 60, %args }, $class;
  $self->{t} = AE::timer 0, $self->{int}, sub { $self->flush };
  return $self;
}

sub flush {
  my $self = shift;
  my $r = $self->{redis};

  print "flushing to rrdcached\n";

  $r->smembers("flush", sub {
    my $keys = shift;
    $r->del("flush", sub {});
    for my $key (@$keys) {
      $r->get($key, sub {
        my $value = shift;
        $self->update_or_create($key, $value);
      });
    }
  });
}

sub update_or_create {
  my ($self, $key, $value) = @_;
  my ($id, $stat) = split "-", $key;
  my $file = "$self->{dir}/$id/$stat.rrd";
  my $time = AE::time;
  my $c = $self->{cached};

  if (!-e $file) {
    return $self->create($id, $stat, sub {
      $c->update($file, "$time:$value")
    });
  }

  $c->update($file, "$time:$value");
}

sub handle_req {
  my ($self, $req) = @_;

  my ($id, $action) = split "/", substr $req->path, 1;
  return $req->error("missing id") unless defined $id;

  do {
    given ($action) {
      when ("incr") { $self->incr($id, $req) }
      when ("stat") { $self->fetch($id, $req) }
      default { $req->error("invalid action") }
    }
  };
}

sub incr {
  my ($self, $id, $req) = @_;
  my @stats = $req->parameters->get_all("stats");
  for my $stat (@stats) {
    my $key = "$id-$stat";
    $self->{redis}->incr($key, sub {
      $self->{redis}->sadd("flush", $key, sub {});
    });
  }
  $req->respond("ok");
}

sub create {
  my ($self, $id, $stat, $cb) = @_;
  my $file = "$self->{dir}/$id/$stat.rrd";

  die "file already exists" if -e $file;
  mkdir "$self->{dir}/$id" unless -d "$self->{dir}/$id";

  rrd_create $file, {
      start => time,
      step  => $self->{int},
    },
    "DS:$stat:COUNTER:120:0:10000",
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
      daemon => $self->{cached}->daemon_addr,
    },
    sub {
      my $samples = shift;
      $_->[1] ||= 0 for @$samples;
      $self->{redis}->get("$id-$stat", sub {
        my $total = shift;
        $req->respond({
          samples => ($samples || []),
          total   => ($total || 0),
        });
      });
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
