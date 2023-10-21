#!/usr/bin/env python3
"""A script that will run benchmark with a some certain thread parameters on a given directory."""
import argparse
import datetime
import os
import shutil
import subprocess
import time
import logging

from rich.logging import RichHandler
import yaml
import psutil

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s', handlers=[RichHandler()])


class Run(object):
    def __init__(self, run_name, config_dict, outdir, clone, root_cmd):
        self.name = run_name
        self.timestamp = time.strftime("%Y%m%d-%H%M%S")
        self.clone = clone # clone number

        # Directories
        self.run_parent_dir = os.path.join(outdir, run_name)
        self.run_output_dir = os.path.join(outdir, run_name, self.timestamp)
        self.command_output_dir = os.path.join(outdir, run_name, 'command_output')
        # Files
        self.time_output = os.path.join(outdir, run_name, self.timestamp, 'time_output.txt')
        self.parent_exitcode_f = os.path.join(outdir, run_name, 'exitcode.txt')
        self.exitcode_f = os.path.join(outdir, run_name, self.timestamp, 'exitcode.txt')
        self.stats_f = os.path.join(outdir, run_name, self.timestamp, 'stats.yaml')
        self.config = config_dict
        self.threads_reading = config_dict['threads_reading']
        self.threads_processing = config_dict['threads_processing']
        self.threads_writing = config_dict['threads_writing']
        self.parallel_runs = config_dict['parallel_runs']
        self.time_to_sleep = 10
        self.root_cmd = root_cmd
        self.command = self.generate_command()

        self.pid = None
        self.subprocess_p = None
        self.psutil_p = None
        self.stats = None
        self.returncode = None

    def generate_command(self):
        """Generates the command to run the benchmark"""
        try:
            command = self.root_cmd.format(**self.__dict__)
        except KeyError as e:
            logging.error(f'Could not generate command for run {self.name}')
            logging.error(e)
            raise
        logging.info(f"Set up command: {command}")
        return command

    def setup_directories(self):
        # output directory inside run dir
        os.makedirs(self.run_output_dir, exist_ok=True)

        # command output directory inside output directory
        os.makedirs(self.command_output_dir, exist_ok=True)


    def already_run(self):
        """Will check if the run has an exitcode file. If it does, it has already been run."""

        if os.path.exists(self.parent_exitcode_f):
            return True
        else:
            return False

    def cleanup_output(self):
        """Remove all the fastq and such, but keep the exitcode file"""
        logging.info(f'Cleaning up output for run {self.name}, deleting {self.command_output_dir}')
        if os.path.exists(self.command_output_dir):
            shutil.rmtree(self.command_output_dir)

    def end_iteration(self):
        logging.info(f'Process has terminated with returncode {self.returncode}')
        with open(self.exitcode_f, 'w') as f:
            f.write(str(self.returncode))
        logging.info(f"Wrote exitcode to file {self.exitcode_f}")
        with open(self.parent_exitcode_f, 'w') as f:
            f.write(str(self.returncode))
        logging.info(f"Wrote exitcode to file {self.parent_exitcode_f}")

        with open(self.stats_f, 'w') as f:
            yaml.safe_dump(self.stats, f)

    def collect_stats_iteration(self):
        stats_d = {}
        try:
            # Collect process specific metrics
            with self.psutil_p.oneshot():
                stats_d['process_cpu_percent'] = self.psutil_p.cpu_percent()
                stats_d['process_memory_percent'] = self.psutil_p.memory_percent()
        # Warnings and not an error since these seem to happen when the process has been finished
        except psutil.ZombieProcess:
            logging.warning(f'Process {self.pid} is a zombie.')
        except psutil.NoSuchProcess:
            logging.warning(f'Process {self.pid} does not exist. (probably exited)')
        return stats_d


def collect_system_stats():
    stats_d = {}
    stats_d['system_cpu_percent'] = psutil.cpu_percent()
    memory_full = psutil.virtual_memory()
    stats_d['system_memory_percent'] = memory_full.percent
    stats_d['system_memory_available'] = memory_full.available
    stats_d['system_memory_total'] = memory_full.total
    return stats_d

def run_parallel(parent_name, runs, sampling_rate, log_per_iterations):
    """Runs a list of runs in parallel. Make sure the runs have different names"""
    logging.info(f'Starting {len(runs)} runs of parent: {parent_name}')

    for run in runs:
        import subprocess

        with open(f'{run.run_output_dir}/stdout.txt', 'w') as stdout_file, open(f'{run.run_output_dir}/stderr.txt', 'w') as stderr_file:
            run.subp_p = subprocess.Popen(['time', '-o', run.time_output] + run.command.split(' '), stdout=stdout_file, stderr=stderr_file)
        run.pid = run.subp_p.pid

        run.psutil_p = psutil.Process(run.pid)

        run.stats = {}

    iteration = 0
    while True:
        # Collect system wide metrics
        timestamp = datetime.datetime.now().isoformat()
        sys_stats = collect_system_stats()
        for run in runs:
            # merge sys_stats dictionary with the returning dictionary
            run.stats[timestamp] = sys_stats | run.collect_stats_iteration()

            if (iteration != 0) and (iteration % log_per_iterations == 0):
                logging.info(f'Run {run.name} still running after {iteration} iterations.')

        # Sleep so that we don't collect too many stats
        time.sleep(sampling_rate)
        iteration += 1



        for run in runs:
            if run.returncode is not None:
                continue

            # Check if iteration should end
            returncode = run.subp_p.poll()
            if returncode is not None:
                run.returncode = returncode
                run.end_iteration()

        # if all run.returncode are not None, then all processes have terminated
        all_terminated = all([run.returncode is not None for run in runs])
        if all_terminated:
            logging.info(f'All runs of parent: {parent_name} have finished.')
            return

def main(args, root_cmd):
    # Read in the yaml config
    with open(args.run_parameter_file, 'r') as conf_fh:
        config = yaml.safe_load(conf_fh)

    runs = config['runs']

    if args.develop:
        sampling_rate = 1
        log_per_iterations = 5
    else:
        sampling_rate = 60
        log_per_iterations = 5

    # For each run
    runs_per_parent = {}
    for run_d in runs:
        parent_run_name = run_d['name']

        runs_per_parent[parent_run_name] = []
        for i in range(run_d['parallel_runs']):
            run = Run(run_d['name'] + f'_clone{i}', run_d, args.output_dir, clone=i, root_cmd=root_cmd)
            run.setup_directories()
            runs_per_parent[parent_run_name].append(run)

        # Check if it has already been run
        if not all([run.already_run() for run in runs_per_parent[parent_run_name]]):
            # Run it and wait for it to finish
            if run_d['parallel_runs'] == 1:
                logging.info(f'Running run with name {run.name}')
            else:
                logging.info(f'Running {run_d["parallel_runs"]} runs with parent name {parent_run_name}')
            run_parallel(parent_run_name, runs_per_parent[parent_run_name], sampling_rate, log_per_iterations)
        else:
            logging.info(f'Run {run.name} has already been run. Skipping.')

        # clean up the old run or the new one
        for run in runs_per_parent[parent_run_name]:
            run.cleanup_output()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('run_parameter_file', help='The file containing the run parameters.')
    parser.add_argument('output_dir', help='The directory to output the stats and fastq files to.')
    parser.add_argument('--develop', action="store_true", help='Whether to run in development mode. Affects the sampling rate and logging of iterations')
    args = parser.parse_args()

    if args.develop:
        ROOT_CMD = "sleep {time_to_sleep}"
    else:
        ROOT_CMD = "/home/hiseq.bioinfo/src/bcl2fastq_v2.20.0.422/bin/bcl2fastq --output-dir {command_output_dir} --loading-threads {threads_reading} --processing-threads {threads_processing} --writing-threads {threads_writing} --create-fastq-for-index-reads --no-lane-splitting --sample-sheet /srv/ngi_data/sequencing/NovaSeqXPlus/nosync/20231018_LH00217_0017_A225J2CLT3/SampleSheet_1.csv --use-bases-mask 4:Y85N66,I10N9,I10,Y133N18 --use-bases-mask 5:Y85N66,I10N9,I10,Y133N18"

    main(args, ROOT_CMD)