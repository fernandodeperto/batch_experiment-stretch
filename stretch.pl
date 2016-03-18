#!/usr/bin/env perl
use strict;
use warnings;

use threads;
use threads::shared;
use Thread::Queue;

use Data::Dumper qw(Dumper);
use Log::Log4perl qw(get_logger);
use Scalar::Util qw(blessed);

use Util qw(git_version);
use Debug;

use Trace;
use Backfilling;

use Basic;
use BestEffortContiguous;
use ForcedContiguous;
use BestEffortLocal;
use ForcedLocal;
use BestEffortPlatform qw(SMALLEST_FIRST BIGGEST_FIRST);
use ForcedPlatform;

my ($execution_id) = @ARGV;

my $trace_file_name = '../swf/CEA-Curie-2011-2.1-cln-b1-clean2.swf';
my $speedup_benchmark = '../NPB3.3.1/NPB3.3-MPI/bin/cg.B.2';
my $experiment_name = 'stretch';
my $experiment_path = "experiment/$experiment_name";
my $basic_file_name = "$experiment_name-$execution_id";
my $execution_path = "$experiment_path/$basic_file_name";
my $platform_file_name = '/tmp/platform';

#my @jobs_numbers = (600, 800, 1000, 1200, 1400);
#my @jobs_numbers = (400, 500, 600);
my @jobs_numbers = (100, 200, 300, 400);
my $threads_number = 6;
my @platform_levels = (1, 2, 40, 5040);
my @platform_latencies = (3.2e-2, 2e-3, 1e-4);
my $stretch_bound = 10;

mkdir $execution_path unless -d $execution_path;

Log::Log4perl::init('log4perl.conf');
my $logger = get_logger('experiment');

my @results;
share(@results);

$logger->info("starting experiment: $experiment_name");
$logger->info("batch-simulator version " . git_version());

my $platform = Platform->new(\@platform_levels);
$platform->build_platform_xml(\@platform_latencies);
$platform->save_platform_xml($platform_file_name);

$logger->info("generating speedup data");
#$platform->generate_speedup($speedup_benchmark, $platform_file);
$platform->set_speedup(\@platform_latencies);

my @variants = (
	Basic->new(),
	BestEffortContiguous->new(),
	ForcedContiguous->new(),
	BestEffortLocal->new($platform),
	ForcedLocal->new($platform),
	#BestEffortPlatform->new($platform),
	#ForcedPlatform->new($platform),
	#BestEffortPlatform->new($platform, mode => SMALLEST_FIRST),
	#ForcedPlatform->new($platform, mode => SMALLEST_FIRST),
	#BestEffortPlatform->new($platform, mode => BIGGEST_FIRST),
	#ForcedPlatform->new($platform, mode => BIGGEST_FIRST),
);

$logger->info("creating queue\n");
my $q = Thread::Queue->new();
for my $variant_id (0..$#variants) {
	for my $jobs_number (@jobs_numbers) {
		$q->enqueue([$variant_id, $jobs_number]);
	}
}
$q->end();

$logger->info("creating threads");
my @threads = map {threads->create(\&run_instance, $_)} (0..($threads_number - 1));

$logger->info("waiting for threads to finish");
my @thread_returns = map {$_->join()} (@threads);

#$logger->info("writing results to file $execution_path/$basic_file_name.csv");
#write_results_to_file();

$logger->info("done");

sub run_instance {
	my $id = shift;
	my $logger = get_logger('experiment');

	# fix for a weird behavior on Perl where die messages coming from code
	# running inside a thread kills the thread but doesn't notify the main
	# thread or shows anything on the output.
	$SIG{__DIE__} = \&signal;

	while (defined(my $instance = $q->dequeue_nb())) {
		my ($variant_id, $jobs_number) = @{$instance};

		$logger->info("thread $id running $variant_id, $jobs_number");

		my $trace = Trace->new_from_swf($trace_file_name);
		$trace->remove_large_jobs($platform->processors_number());
		$trace->keep_first_jobs($jobs_number);
		$trace->fix_submit_times();
		$trace->reset_jobs_numbers();

		my $schedule = Backfilling->new($variants[$variant_id], $platform, $trace);
		$schedule->run();

		##DEBUG_BEGIN
		$logger->debug("thread $id finished $variant_id, $jobs_number");
		$trace->write_to_file("$execution_path/$basic_file_name-$variant_id-$jobs_number.swf");
		$schedule->save_svg("$execution_path/$basic_file_name-$variant_id-$jobs_number.svg");
		write_results($schedule, "$execution_path/$basic_file_name-$variant_id-$jobs_number.csv");
		##DEBUG_END

		#my @instance_results = (
		#	$platform->processors_number(),
		#	$jobs_number,
		#	$platform->cluster_size(),
		#	#blessed $variants[$variant_id],
		#	$variant_id,
		#	$schedule->cmax(),
		#	$schedule->contiguous_jobs_number(),
		#	$schedule->local_jobs_number(),
		#	$schedule->locality_factor(),
		#	$schedule->bounded_stretch($stretch_bound),
		#	#$schedule->stretch_sum_of_squares($stretch_bound),
		#	#$schedule->stretch_with_cpus_squared($stretch_bound),
		#	$schedule->run_time(),
		#	$schedule->platform_level_factor(),
		#	$schedule->job_success_rate(),
		#);
		#push @results, join(' ', @instance_results);
	}

	$logger->info("thread $id finished");
	return;
}

sub write_results {
	my $schedule = shift;
	my $file_name = shift;

	my $trace = $schedule->trace();
	my $jobs = $trace->jobs();

	open(my $file, '>', $file_name);

	print $file join(' ', (
			"JOB_NUMBER",
			"SUBMIT_TIME",
			"WAIT_TIME",
			"RUN_TIME",
			"REQUESTED_TIME",
			"BSLD",
			"AVG_BSLD",
	)) . "\n";

	my $total_bounded_stretch = 0;
	my $processed_jobs = 0;

	for my $job (@{$jobs}) {
		$total_bounded_stretch += $job->bounded_stretch_based_on_requested_time($stretch_bound);
		$processed_jobs++;

		print $file join(' ', (
			$job->job_number(),
			$job->submit_time(),
			$job->wait_time(),
			$job->run_time(),
			$job->requested_time(),
			$job->bounded_stretch_based_on_requested_time($stretch_bound),
			$total_bounded_stretch/$processed_jobs,
		)) . "\n";
	}

	close($file);
	return;
}

sub signal {
	my $signal = shift;
	print STDERR "$signal\n";
}

sub get_log_file {
	return "$execution_path/$basic_file_name.log";
}

sub write_results_to_file {
	my $file_name = "$execution_path/$basic_file_name.csv";
	open(my $file, '>', $file_name) or $logger->logdie("unable to create results file $file_name");

	print $file join(' ', (
			"CPUS_NUMBER",
			"JOBS_NUMBER",
			"CLUSTER_SIZE",
			"VARIANT",
			"CMAX",
			"CONT_JOBS",
			"LOC_JOBS",
			"LOC_FACTOR",
			"BOUNDED_STRETCH",
			#"STRETCH_SUM_SQUARES",
			#"STRETCH_CPUS_SQUARED",
			"RUN_TIME",
			"PLATFORM_LEVEL",
			"SUCCESS_RATIO",
		)) . "\n";

	print $file join(' ', $_) . "\n" for (@results);
	return;
}

