package Dragon::Scales::Request;

use parent "Plack::Request";
use JSON::XS;

sub new {
  my ($class, $env, $respond) = @_;
  my $self = $class->SUPER::new($env);
  die "respond callback required" unless defined $respond;
  $self->{respond} = $respond;
  return $self;
}

sub api_response {
  my $data = shift;
  return [200, ["Content-Type", "text/json"], [encode_json $data]];
}

sub respond {
  my ($self, $data) = @_;
  $self->{respond}->(
    api_response {
      data => $data,
      success => JSON::XS::true,
    }
  );
}

sub error {
  my ($self, $message) = @_;
  $self->{respond}->(
    api_response {
      error => $message,
      success => JSON::XS::false,
    }
  );
}

sub notfound {
  $self->{respond}->(
    [404, ["Content-Type", "text/plain"], ["not found"]]
  );
}

1;
