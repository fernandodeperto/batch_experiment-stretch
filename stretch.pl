#!/usr/bin/env perl
use strict;
use warnings;

use Data::Dumper qw(Dumper);
use Log::Log4perl qw(get_logger);

use Util qw(git_version);
use Trace;
use Backfilling;
use Basic;
use BestEffortContiguous;
use ForcedContiguous;
use BestEffortLocal;
use ForcedLocal;
use BestEffortPlatform qw(SMALLEST_FIRST BIGGEST_FIRST);
use ForcedPlatform;

my ($trace_file, $jobs_number) = @ARGV;

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger();

$logger->info("running batch-simulator version " . git_version());

my @platform_levels = (1, 2, 40, 5040);
my @platform_latencies = (3.2e-2, 2e-3, 1e-4);
my $speedup_benchmark = '../NPB3.3.1/NPB3.3-MPI/bin/cg.B.2';
my $platform_file = '/tmp/platform';

my $platform = Platform->new(\@platform_levels);
$platform->build_platform_xml(\@platform_latencies);
$platform->save_platform_xml($platform_file);
$platform->generate_speedup($speedup_benchmark, $platform_file);
#$platform->set_speedup(\@platform_latencies);

#my $trace = Trace->new_from_trace(Trace->new_from_swf($trace_file), $jobs_number);
my $trace = Trace->new_from_swf($trace_file);
$trace->remove_large_jobs($platform->processors_number());
$trace->reset_jobs_numbers();
$trace->fix_submit_times();
$trace->keep_first_jobs($jobs_number) if defined $jobs_number;

my $reduction_algorithm = BestEffortPlatform->new($platform);

my $schedule = Backfilling->new($reduction_algorithm, $platform, $trace);
$schedule->run();

my @results = (
	$schedule->cmax(),
	$schedule->contiguous_jobs_number(),
	$schedule->local_jobs_number(),
	#$schedule->locality_factor(),
	#$schedule->stretch_sum_of_squares(),
	#$schedule->stretch_with_cpus_squared(),
	#$schedule->run_time(),
);

print STDOUT join(' ', @results) . "\n";
$schedule->save_svg('svg/run_schedule.svg');

sub get_log_file {
	return 'log/run_schedule.log';
}
