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
    handle => undef,
    conn   => undef,
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

sub handle {
  my ($self, $cv, $attempts) = @_;
  $cv ||= AE::cv;

  if ($self->{handle}) {
    $cv->send($self->{handle});
    return $cv;
  }

  $self->{conn} = tcp_connect "unix/", "$self->{dir}/rrd.sock", sub {
    my ($fh) = @_;

    if (!$fh) {
      $self->spawn->cb(sub {
        eval { shift->recv };

        if ($@) {
          $cv->croak($@);
        }
        elsif ($attempts) {
          $cv->croak("spawned but can not connect");
        }
        else {
          $self->handle($cv, $attempts++);
        }
      });
    }
    else {
      $self->{handle} = AnyEvent::Handle->new(
        fh => $fh,
        on_eof   => sub { delete $self->{handle} },
        on_error => sub { delete $self->{handle} },
      );
      $cv->send($self->{handle});
    }
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

1;
