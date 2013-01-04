package Dragon::Scales;

use v5.14;

use AnyEvent;
use AnyEvent::Util;
use Storable ();
use Dragon::Scales::Util;
use Dragon::Scales::Request;

sub new {
  my ($class, %args) = @_;
  for (qw{dir cached}) {
    die "$_ is required" unless defined $args{$_};
  }

  my $self = bless {
    interval => 60,
    flush => {},
    %args
  }, $class;

  $self->load_totals;
  $self->{t} = AE::timer 0, $self->{interval}, sub { $self->flush };

  return $self;
}

sub flush {
  my $self = shift;
  print "flushing to rrdcached\n";

  for my $id (keys %{$self->{flush}}) {
    for my $stat (keys %{$self->{flush}{$id}}) {
      my $value = $self->{total}{$id}{$stat};
      $self->update_or_create($id, $stat, $value);
    }
    delete $self->{flush}{$id};
  }

  $self->store_totals;
}

sub update_or_create {
  my ($self, $id, $stat, $value) = @_;
  my $file = $self->rrd_path($id, $stat);
  my $time = AE::time;
  my $c = $self->{cached};

  if (-e $file) {
    return $c->update($file, "$time:$value");
  }

  $self->create($id, $stat, sub {
    $c->update($file, "$time:$value")
  });
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
    $self->{total}{$id}{$stat}++;
    $self->{flush}{$id}{$stat} ||= 1;
  }
  $req->respond("ok");
}

sub create {
  my ($self, $id, $stat, $cb) = @_;
  my $file = $self->rrd_path($id, $stat, 1);

  die "file already exists" if -e $file;

  rrd_create $file, {
      start => time,
      step  => $self->{interval},
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
  my $file = $self->rrd_path($id, $stat);
  my $time = time;

  rrd_fetch $file, {
      start => $time - 3600,
      end   => $time,
      daemon => $self->{cached}->daemon_addr,
    },
    sub {
      my $samples = shift;
      $_->[1] ||= 0 for @$samples;

      $req->respond({
        samples => ($samples || []),
        total   => ($self->{total}{$id}{$stat} || 0),
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

sub rrd_path {
  my ($self, $id, $stat, $create) = @_;
  my @dirs = ($self->{dir}, split "", sprintf("%04x", $id), $id);

  if ($create) {
    mkdir join "/", @dirs[0 .. $_] for 0 .. @dirs - 1;
  }

  join "/", @dirs, "$stat.rrd";
}

sub load_totals {
  my $self = shift;
  my $file = "$self->{dir}/totals";

  $self->{total} = -e $file ? Storable::retrieve $file : {};
}

sub store_totals {
  my $self = shift;
  my $file = "$self->{dir}/totals";
  print "writing totals to $file\n";

  Storable::store $self->{total}, $file;
}

sub DESTROY {
  my $self = shift;
  $self->store_totals;
}

1;
