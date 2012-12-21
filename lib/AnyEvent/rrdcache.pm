package AnyEvent::rrdcache;

use v5.14;

use Cwd ();
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
  my ($class, $connect) = @_;
  die "need connect array ref" unless defined $connect and @$connect == 2;
  bless {connect => $connect}, $class;
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
    $h->push_write("AnyEvent::Handle::rrdcache", $command, @args);
    $h->push_read("AnyEvent::Handle::rrdcache" => sub {
      if ($command eq "BATCH") {
        $cv->send(AnyEvent::rrdcache::Batch->new($_[0]));
      }
      else {
        $cv->send($_[1]);
      }
    });
  });

  return $cv;
}

sub connect {
  my ($self, $cv, $attempts) = @_;
  $cv ||= AE::cv;

  my ($host, $port) = @{$self->{connect}};

  $self->{conn} = tcp_connect $host, $port, sub {
    my ($fh) = @_;

    if (!$fh) {
      $cv->croak("cound not connect to rrdcached at " . join " ", @{$self->{connect}});
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

package AnyEvent::Handle::rrdcache;

sub anyevent_read_type {
  my ($handle, $cb) = @_;
  my ($lines, $msg);

  sub {
    print $_[0]{rbuf};
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
  return join(" ", @args) . "\n";
}

package AnyEvent::rrdcache::Batch;

use parent "AnyEvent::rrdcache";

sub new {
  my ($class, $handle) = @_;
  die "handle required" unless defined $handle;
  bless {
    h => $handle,
    commands => [],
  }, $class;
}

sub command {
  my $self = shift;
  push @{$self->{commands}}, join " ", @_;
  $self->{h}->push_write("AnyEvent::Handle::rrdcache", @_);
}

sub complete {
  my ($self, $cb) = @_;
  my $cv = AE::cv;

  if ($cb) {
    $cv->cb(sub {
      my $ret = eval { shift->recv };
      return $cb->(undef, $@) if $@;
      $cb->($ret);
    });
  }

  $self->{h}->push_write("AnyEvent::Handle::rrdcache", ".");
  $self->{h}->push_read("AnyEvent::Handle::rrdcache", sub {
    $cv->send($_[1]);
    $_[0]->destroy;
    undef $_[0];
  });

  return $cv;
}

1;
