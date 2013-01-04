package AnyEvent::rrdcache;

use v5.14;

use Data::Dump qw/pp/;
use Scalar::Util qw/weaken/;
use AnyEvent::Socket;
use AnyEvent::Handle;

BEGIN {
  our @COMMANDS = qw/
  flush flushall pending forget queue
  help stats update wrote batch quit/;

  for my $cmd (@COMMANDS) {
    no strict "refs";
    *{"AnyEvent::rrdcache::$cmd"} = sub {
      shift->command(uc $cmd, @_)
    };
  }
}

sub new {
  my ($class, %args) = @_;
  die "host is required" unless defined $args{host};
  die "port is required" unless defined $args{port};
  bless {%args, connect_queue => []}, $class;
}

sub daemon_addr {
  my $self = shift;
  my $host = $self->{host} eq "unix/" ? "unix" : $self->{host};
  return "$host:$self->{port}";
}

sub command {
  my $self = shift;
  my $cmd = shift;
  $self->{cmd_cb} or return $self->connect($cmd, @_);
  $self->{cmd_cb}->($cmd, @_);
}

sub cleanup {
  my ($self, $msg) = @_;
  delete $self->{conn};
  delete $self->{cmd_cb};
}

sub connect {
  my ($self) = shift;

  my $cv;
  if (@_) {
    $cv = pop if UNIVERSAL::isa($_[-1], 'AnyEvent::CondVar');
    $cv ||= AE::cv;
    push @{$self->{connect_queue}}, [ $cv, @_ ];
  }

  return $cv if $self->{conn};

  weaken $self;

  $self->{conn} = tcp_connect $self->{host}, $self->{port}, sub {
    my ($fh) = @_;

    if (!$fh) {
      my $err = "could not connect to rrdcached";
      $self->cleanup($err);
      $cv->croak($err);
      return;
    }

    my $h = AnyEvent::Handle->new(
      fh => $fh,
      on_eof => sub {
        $_[0]->destroy;
        $self->cleanup($_[2]) if $_[1];
      },
      on_read => sub {
        $_[0]->destroy;
        $self->cleanup("connection closed");
      },
    );

    $self->{cmd_cb} = sub {
      my $cmd = shift;
      my ($cv, $cb);
      if (@_) {
        $cv = pop if ref $_[-1] && UNIVERSAL::isa($_[-1], 'AnyEvent::CondVar');
        $cb = pop if ref $_[-1] eq 'CODE';
      }
      $cv ||= AE::cv;
      $cv->cb(sub {
        my $cv = shift;
        my $res = $cv->recv;
        $cb->($res) if $cb;
      });
      $h->push_write("AnyEvent::Handle::rrdcache", $cmd, @_);
      if ($cmd eq "QUIT") {
        $h->on_eof(sub { $cv->send("connection closed") });
      }
      else {
        $h->push_read("AnyEvent::Handle::rrdcache", sub {
          $cv->send($_[1]);
        });
      }
    };

    my $queue = delete $self->{connect_queue} || [];
    for my $command (@$queue) {
      my ($cv, @args) = @$command;
      $self->{cmd_cb}->(@args, $cv);
    }
  };

  return $cv;
}

package AnyEvent::Handle::rrdcache;

sub anyevent_read_type {
  my ($handle, $cb) = @_;
  my ($lines, @lines, $msg);

  sub {
    if (!$lines && $_[0]{rbuf} =~ s/^([0-9]+|-1) (.*)\n//) {
      ($lines, $msg) = ($1, $2);
      if ($lines eq "0") {
        $lines = undef;
        $cb->($_[0], [$msg]);
        return 1;
      }
      if ($lines eq "-1") {
        $lines = undef;
        $_[0]->_error(Errno::EBADMSG, 1, $msg);
        return 1;
      }
    }

    if ($lines) {
      while ($_[0]{rbuf} =~ s/(.*)\n//) {
        push @lines, $1;
        if (@lines == $lines) {
          $cb->($_[0], \@lines);
          $lines = undef;
          @lines = ();
          return 1;
        }
      }
    }
  }
};

sub anyevent_write_type {
  my ($handle, @args) = @_;
  return join(" ", @args) . "\n";
}

1;
