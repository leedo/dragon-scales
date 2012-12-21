package DB::rrdcached;

use v5.14;

use Cwd ();
use AnyEvent::Socket;
use AnyEvent::Handle;

sub new {
  my ($class, $dir) = @_;

  $dir = $dir ? Cwd::abs_path($dir) : Cwd::getcwd . "/cached";
  bless {
    dir    => $dir,
    pid    => undef,
  }, $class;
}

sub args {
  my $self = shift;
  return [ qw/rrdcached -g -w 60 -z 10 -m 0644/,
           "-b", "$self->{dir}",
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
      if ($_[0] =~ /listening/) {
        $cv->send;
        undef $cv;
      }
    };

  $cmd->cb(sub {
    my $ret = shift->recv;
    $cv->croak("rrdcached died with $ret") if $cv;
  });

  return $cv;
}

sub command {
  my ($self, $command) = @_;
  my $cv = AE::cv;

  my $handle = $self->handle;
  $handle->cb(sub {
    my $h = eval { shift->recv };
    return $cv->croak($@) if $@;
    $h->push_write("AnyEvent::Handle::rrdcached", $command);
    $h->push_read("AnyEvent::Handle::rrdcached" => sub {
      $cv->send($_[1]);
    });
  });

  return $cv;
}

sub handle {
  my ($self, $cv, $attempts) = @_;
  $cv ||= AE::cv;

  $self->{conn} = tcp_connect "unix/", "$self->{dir}/rrd.sock", sub {
    my ($fh) = @_;

    if (!$fh) {
      $cv->croak("cound not connect to rrdcached at $self->{dir}/rrd.sock");
      return;
    }

    $self->{handle} = AnyEvent::Handle->new(
      fh => $fh,
      on_eof   => sub { delete $self->{handle} },
      on_error => sub { delete $self->{handle} },
    );

    $cv->send($self->{handle});
  };

  return $cv;
}

sub DESTROY {
  my $self = shift;
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

package AnyEvent::Handle::rrdcached;

sub anyevent_read_type {
  my ($handle, $cb) = @_;
  my ($lines, $msg);

  sub {
    if (!$lines && $_[0]{rbuf} =~ s/^(\d+) (.*)\n//) {
      ($lines, $msg) = ($1, $2);
      if ($lines eq "0") {
        $cb->($_[0], [$msg]);
        return 1;
      }
    }

    if ($lines) {
      my @lines = split "\n", $_[0]{rbuf};
      if (@lines == $lines) {
        $cb->($_[0], \@lines);
        return 1;
      }
    }
  }
};

sub anyevent_write_type {
  my ($handle, @args) = @_;
  return join(" ", @args) . "\n";
}

1;
