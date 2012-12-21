package Dragon::Scales;

use v5.14;

use Dragon::Scales::Request;

sub new {
  my ($class, $client) = @_;
  die "need rrdcache client" unless defined $client;
  bless {client => $client}, $class;
}

sub handle_req {
  my ($self, $req) = @_;

  my ($id) = split "/", substr $req->path, 1;
  return $req->error("missing id") unless defined $id;

  do {
    given ($req->method) {
      when ("PUT") { $self->update($id, $req) }
      #when ("GET") { $self->fetch($id, $req)  }
      default      { $self->update($id, $req) }
    }
  };
}

sub update {
  my ($self, $id, $req) = @_;

  my @stats = $req->parameters->get_all("stats");

  $self->{client}->batch(sub {
    my ($batch, $err) = @_;
    return $req->error($err) if $err;

    $batch->update("$id-$_.rrd", 1) for @stats;

    $batch->complete(sub {
      my ($res, $err) = @_;
      return $req->error($err) if $err;
      $req->respond($res);
    });
  });
}

sub fetch {
  my ($self, $id, $req) = @_;
  my @stats = $req->parameters->get_all("stats");
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
