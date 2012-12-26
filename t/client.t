#!/usr/bin/env perl

use Test::More;
use AnyEvent::rrdcache::Test;

test_rrdcache sub {
  my $client = shift;
  my $cv = AE::cv;
  isa_ok $client, AnyEvent::rrdcache;
  $cv->begin;
  $client->help(sub {
    is ref $_[0], "ARRAY";
    $cv->end;
  });
  $cv->begin;
  $client->help(sub {
    is ref $_[0], "ARRAY";
    $cv->end;
  });

  $cv->begin;
  $client->quit(sub {
    is $_[0], "connection closed";
    $cv->end;
  });
  $cv->recv;
  done_testing();
};
