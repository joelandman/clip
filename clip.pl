#!/opt/scalable/bin/perl

# Copyright (c) 2012-2015 Scalable Informatics
# This is free software, see the gpl-2.0.txt
# file included in this distribution


use strict;
use English '-no_match_vars';
use Getopt::Lucid qw( :all );
use POSIX qw[strftime];
use SI::Utils;
use File::Path;
use lib "lib/";
use IPC::Run qw( start pump finish timeout run harness );
use Data::Dumper;
use IO::Handle;
use File::Spec;
use Time::HiRes qw( usleep ualarm gettimeofday tv_interval nanosleep
                             clock_gettime clock_getres clock_nanosleep clock
                             stat );
use Config::JSON;
use Digest::SHA  qw(sha256_hex);

use threads;
use threads::shared;

#
my $vers    = "0.8";

# variables
my ($opt,$rc,$version,$thr);
my $debug 		        : shared;
my $verbose           : shared;
my $help;
my $dir               : shared;
my $system_interval	  : shared;
my $block_interval    : shared;
my $done              : shared;
my $timestamp         : shared;
my $hostname          : shared;
my $run_dir           : shared;
my ($thr_name);
my ($proto,$port);
my ($host,$user,$pass,$output,$config_file,$cf_h);
my ($config,$c,$_fqpni,$nolog);
my $shared_sig	      : shared;

chomp($hostname   = `hostname`);


# signal catcher
# SIGHUP is a graceful exit, SIGKILL and SIGINT are immediate
my (@sigset,@action);
sub sig_handler_any {
	our $out_fh;
	$shared_sig++;
	print STDERR "caught termination signal\n";
  $done   = true;
	printf STDERR "Waiting 10 seconds to clean up\n";
  if ($shared_sig == 1) {	sleep 10; }
  die "thread caught termination signal\n";
}
$SIG{HUP} = \&sig_handler_any;
$SIG{KILL} = \&sig_handler_any;
$SIG{INT} = \&sig_handler_any;
$SIG{QUIT} = \&sig_handler_any;

my @command_line_specs = (
		     Param("config|c"),
                     Switch("help"),
                     Switch("version"),
                     Switch("debug"),
                     Switch("verbose"),
		                 Switch("nolog"),
                     );

# parse all command line options
eval { $opt = Getopt::Lucid->getopt( \@command_line_specs ) };
if ($@)
  {
    #print STDERR "$@\n" && help() && exit 1 if ref $@ eq 'Getopt::Lucid::Exception::ARGV';
    print STDERR "$@\n\n" && help() && exit 2 if ref $@ eq 'Getopt::Lucid::Exception::Usage';
    print STDERR "$@\n\n" && help() && exit 3 if ref $@ eq 'Getopt::Lucid::Exception::Spec';
    #printf STDERR "FATAL ERROR: netmask must be in the form x.y.z.t where x,y,z,t are from 0 to 255" if ($@ =~ /Invalid parameter netmask/);
    ref $@ ? $@->rethrow : die "$@\n\n";
  }

# test/set debug, verbose, etc
$debug      = $opt->get_debug   ? true : false;
$verbose    = $opt->get_verbose ? true : false;
$help       = $opt->get_help    ? true : false;
$version    = $opt->get_version ? true : false;
$nolog	    = $opt->get_nolog   ? true : false;
$config_file= $opt->get_config  ? $opt->get_config  : 'clip.json';

$done       	= false;

&help()             if ($help);
&version($vers)     if ($version);

$config 	  = &parse_config_file($config_file);
# run_dir from command line takes precedence over config file
my $toppath    = $config->{'config'}->{'global'}->{'toppath'};
my $cliplen    = $config->{'config'}->{'global'}->{'length'};

# start time stamp thread
$thr->{TS}                      = threads->create({'void' => 1},'TS');

# loop through all the machines in the config file, and
my $cameras 	= $config->{'config'}->{'cameras'};

foreach my $camera (sort keys %{$cameras})
   {
      my $url 		= $cameras->{$camera}->{'url'};
      $thr_name 	= sprintf 'stream.%s',$camera;
      my $tpath = File::Spec->catfile($toppath,$camera);
      printf "D[%i]  camera: %s -> \'%s\'\n",$$,$thr_name,$tpath if ($debug);
      if ($cameras->{$camera}->{enabled} == 1) {
          printf "D[%i]    -: starting streaming thread for camera->%s\n",$$,$camera;
          $thr->{$thr_name}	= threads->create({'void' => 1},'clip',$camera,$tpath,$cliplen,$url);
      }
   }


# main loop ... sleep for 100k us (0.1 s)
do {
    usleep(100000);
} until ($done);

foreach my $_tn (keys %{$thr}) {
 $thr->{$_tn}->join();
}

exit 0;

sub version {
    my $V = shift;
    print "new.pl version $V\n";
    exit 0;
}

sub clip {
    my ($cam,$top,$len,$url) = @_;

    $done	          = false;

    # microseconds to sleep before waking, defaults to 0.25 seconds
    my $interval    = 250000;

    # microseconds for timeout of command, defaults to 1 seconds
    my $timeout     = 1000000;

    my ($in,$out,$err,@lines,@times,$ts,$h,$ipmi,@cmd,$out_fh,$out_fn);
    my ($lineout,$r,$mname,$mvalue,$dval,$date,$dest,$running,$_command);
    my ($sout,$date,@lt,$time,$dout);

    my ($dt,$t0,$tf);

    $dest	  = $top;
    $running= false;
    printf  "D[%i]   thread=%s: dest=\'%s\'\n",$$,$thr_name,$dest if ($debug);
    printf  "D[%i]   thread=%s: len=\'%s\'\n",$$,$thr_name,$len if ($debug);

    # len sanity
    $len = 60   if ($len < 1);
    $len = 3599 if ($len > 3600);

    # main loop thread
    do
       {
        if (!$running) {
            @lt       = gmtime;
            $date     = sprintf "%i-%i-%i",$lt[5]+1900,$lt[4],$lt[3];
            $time     = sprintf "%02i:%02i:%02i",$lt[2],$lt[1],$lt[0];
            $dout     = File::Spec->catfile($top,$date,$lt[2]);
            $sout     = sprintf '"%s_%s.mp4"',$date,$time;
            mkpath($dout,1,0755);
            chdir($dout) or die "FATAL ERROR: cannot chdir to $dout\n";
            printf  "D[%i]   thread=%s: directory=\'%s\'\n",$$,$thr_name,$dout if ($debug);
            printf  "D[%i]   thread=%s: file=\'%s\'\n",$$,$thr_name,$sout if ($debug);

            $_command = sprintf "/usr/bin/cvlc  %s --sout %s",$url,$sout;
            #$_command =~ s/\@/\\\@/;
            @cmd      = split(/\s+/,$_command);
            $dt       = 0;
            $t0       = [gettimeofday];
            $out        = "";
            $in         = "";
            printf  "D[%i]    --: thread=%s: command=\'%s\'\n",$$,$thr_name,join(" ",@cmd) if ($debug);
            $h = start \@cmd, \$in, \$out, \$err, timeout($len), debug=>$debug;
            $running    = true;
            }

          eval {
           if ($running) {
            $h->pump_nb if ($h->pumpable);

            $tf = tv_interval($t0,[gettimeofday]);
            if ($tf >= $len) {
              $h->kill_kill;
              $running  = false;
            }
            usleep($interval);
           }
          };

          # catch timeout
          if ( $@ ) {
            my $_to   = $@;
            $h->kill_kill;
            $running  = false;
            usleep($interval);
          }

      } until ($done);
      $h->kill_kill;

}

sub TS {
    my $sleep_interval  = 250000; # microseconds to sleep before waking
    my $last = 0;
    do {
        $timestamp  = time();
        if ((int($timestamp - $last) >= 1)) {
            #printf "D[%i] time: %f\n",$$,$timestamp if ($debug);
            $last = $timestamp;
        }

        usleep($sleep_interval);
    } until ($done);
}

sub parse_config_file {
    my $file	= shift;
    my $rc;
    if (-e $file) {
	if (-r $file) {
		$rc = Config::JSON->new($file);
	}
	else
	{
		die "FATAL ERROR: config file \'$config_file\' exists but is unreadable by this userid\n";
	}

	#code
    }
    else
    {
	die "FATAL ERROR: config file \'$config_file\' does not exist\n";
    }
    return $rc;
}
