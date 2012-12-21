package AnyEvent::rrdcached;

use v5.14;

use Cwd ();
use AnyEvent::Socket;
use AnyEvent::Handle;

our @COMMANDS = qw/
  flush flushall pending forget queue
  help stats update wrote batch quit/;

for my $cmd (@COMMANDS) {
  no strict "refs";
  *{"AnyEvent::rrdcached::$cmd"} = sub {
    shift->command(uc $cmd, @_)
  };
}

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
      print $_[0];
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
  my ($self, $command, @args) = @_;
  my $cv = AE::cv;
  my $cb;

  if (@args) {
    $cb = pop @args if ref $args[-1] eq 'CODE';
  }

  if ($cb) {
    $cv->cb(sub {
      my $ret = eval { shift->recv };
      return $cb->(undef, $@) if $@;
      $cb->($ret);
    });
  }

  my $connect = $self->connect;
  $connect->cb(sub {
    my $h = eval { shift->recv };
    return $cv->croak($@) if $@;
    $h->on_error(sub { $cv->croak($_[2]) });
    $h->push_write("AnyEvent::Handle::rrdcached", $command, @args);
    $h->push_read("AnyEvent::Handle::rrdcached" => sub {
      $cv->send($command eq "BATCH" ? $_[0] : $_[1]);
    });
  });

  return $cv;
}

sub batch_command {
  my ($self, $h, $command, @args) = @_;
  $h->push_write("AnyEvent::Handle::rrdcached", $command, @args);
}

sub batch_complete {
  my ($self, $h, $cb) = @_;
  my $cv = AE::cv;

  if ($cb) {
    $cv->cb(sub {
      my $ret = eval { shift->recv };
      return $cb->(undef, $@) if $@;
      $cb->($ret);
    });
  }

  $h->push_write("AnyEvent::Handle::rrdcached", ".");
  $h->push_read("AnyEvent::Handle::rrdcached", sub {
    $cv->send($_[1]);
  });

  return $cv;
}

sub connect {
  my ($self, $cv, $attempts) = @_;
  $cv ||= AE::cv;

  $self->{conn} = tcp_connect "unix/", "$self->{dir}/rrd.sock", sub {
    my ($fh) = @_;

    if (!$fh) {
      $cv->croak("cound not connect to rrdcached at $self->{dir}/rrd.sock");
      return;
    }

    my $h; $h = AnyEvent::Handle->new(
      fh => $fh,
      on_eof => sub { undef $h }
    );
    $cv->send($h);
  };

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

package AnyEvent::Handle::rrdcached;

sub anyevent_read_type {
  my ($handle, $cb) = @_;
  my ($lines, $msg);

  sub {
    if (!$lines && $_[0]{rbuf} =~ s/^([0-9]+|-1) (.*)\n//) {
      ($lines, $msg) = ($1, $2);
      if ($lines eq "0") {
        $cb->($_[0], [$msg]);
        return 1;
      }
      if ($lines eq "-1") {
        $_[0]->_error(Errno::EBADMSG, 1, $msg);
        return;
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
  warn join " ", @args;
  return join(" ", @args) . "\n";
}

1;
