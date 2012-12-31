package AnyEvent::rrdcached;

use AnyEvent;
use AnyEvent::Util ();

sub new {
  my ($class, $dir) = @_;
  mkdir "$dir/$_" for qw/journal/;

  bless {
    dir    => $dir,
    pid    => undef,
  }, $class;
}

sub port {
  my $self = shift;
  return "$self->{dir}/rrd.sock";
}

sub host {
  my $self = shift;
  return "unix/";
}

sub rrd_dir {
  my $self = shift;
  return "$self->{dir}/rrds";
}

sub args {
  my $self = shift;
  # flush each file randomly over an hour
  return [ qw/rrdcached -g -w 3600 -z 3600 -f 7200 -m 0644 -B/,
           "-b", $self->{dir},
           "-l", "unix:$self->{dir}/rrd.sock",
           "-p", "$self->{dir}/rrd.pid",
           "-j", "$self->{dir}/journal" ];
}

sub spawn {
  my $self = shift;
  my $cv = AE::cv;

  my $cmd = AnyEvent::Util::run_cmd $self->args,
    '$$' => \($self->{pid}),
    '2>' => sub {
      print $_[0];
      if ($_[0] =~ /listening/) {
        $cv->send;
        undef $cv;
      }
    };

  $cmd->cb(sub {
    my $ret = shift->recv;
    $cv->croak("rrdcached died with exit code $ret") if $cv;
  });

  return $cv;
}

sub kill {
  my $self = shift;
  return unless $self->{pid};

  my $pid = do {
    open my $fh, "<", "$self->{dir}/rrd.pid";
    local $/;
    <$fh>;
  };
  if ($pid == $self->{pid}) {
    kill 2, $pid;
    waitpid $pid, 0;
  }
}

sub DESTROY {
  my $self = shift;
  $self->kill;
}

1;
