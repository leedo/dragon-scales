package Dragon::Scales;

use v5.14;

use AnyEvent;
use AnyEvent::Util;
use Dragon::Scales::Request;

sub new {
  my ($class, $client, $dir) = @_;
  die "need rrdcache client" unless defined $client;
  die "need rrd dir" unless defined $dir;

  my $self = bless {
    client => $client,
    dir    => $dir,
    buffer => {},
  }, $class;

  $self->{t} = AE::timer 60, 60, sub { $self->flush };

  return $self;
}

sub flush {
  my $self = shift;
  my $cv = AE::cv;
  my $time = AE::time;
  my $format = "rrds/%s.rrd $time:%s";

  for my $stat (keys %{$self->{buffer}}) {
    my $value = delete $self->{buffer}{$stat};
    $self->{client}->update(sprintf($format, $stat, $value), sub {});
  }
}

sub handle_req {
  my ($self, $req) = @_;

  my ($id, $action) = split "/", substr $req->path, 1;
  return $req->error("missing id") unless defined $id;

  do {
    given ($action) {
      when ("pv")     { $self->update($id, $req) }
      when ("stats")  { $self->fetch($id, $req)  }
      when ("create") { $self->create($id, $req) }
      default { $req->respond("invalid action") }
    }
  };
}

sub update {
  my ($self, $id, $req) = @_;
  my @stats = $req->parameters->get_all("stats");

  for my $stat (@stats) {
    $self->{buffer}{"$id-$stat"}++;
  }

  $req->respond("ok");
}

sub create {
  my ($self, $id, $req) = @_;
  my $stat = $req->parameters->{stat};
  my $file = "$id-$stat.rrd";

  if (-e "$self->{dir}/$file") {
    $req->respond("file already exists");
    return;
  }

  my @cmd = (
    qw/rrdtool create/, "$self->{dir}/$file",
    "--start", time, "--step", 60,
    "DS:$stat:GAUGE:120:U:U",
    "RRA:AVERAGE:0.5:5:576", # five minute interval for two days
    "RRA:AVERAGE:0.5:30:1400", # thirty minute interval for a month
    "RRA:AVERAGE:0.5:720:704", # 12 hour interval for a year
  );

  my ($out, $err);
  my $cv = AnyEvent::Util::run_cmd \@cmd,
    "1>" => \$out,
    "2>" => \$err;

  $cv->cb(sub {
    shift->recv && return $req->error("error creating RRD: $err");
    $req->respond("created $file");
  });
}

sub fetch {
  my ($self, $id, $req) = @_;
  my $stat = $req->parameters->{stat};
  my $file = "$self->{dir}/$id-$stat.rrd";
  $self->{client}->flush($file, sub {
    my ($ret, $err) = @_;
    return $req->error($err) if $err;

    my $time = time;
    my @cmd = (
      "rrdtool", "fetch",
      "--start", time - 3600, "--end", time,
      "$self->{dir}/$id-$stat.rrd", "AVERAGE"
    );

    my ($out, $err);
    my $cv = AnyEvent::Util::run_cmd \@cmd,
      "1>" => \$out,
      "2>" => \$err;
    $cv->cb(sub {
      shift->recv and return $req->error("rrdfetch died: $out");
      my @lines = split "\n", $out;
      $req->respond([map {
        my ($time, $val) = split ": ", $_;
        [int($time), $val eq "nan" ? 0 : eval($val)];
      } @lines[2 .. $#lines - 1]]);
    });
  });
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
