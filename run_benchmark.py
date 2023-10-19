#!/usr/bin/env python3
"""A script that will run benchmark with a some certain thread parameters on a given directory."""
import argparse
import os
import subprocess
import time
import logging

import yaml
import psutil

CMD = "sleep 10"

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Run(object):
    def __init__(self, run_name, config_dict):
        self.name = run_name
        self.timestamp = time.strftime("%Y%m%d-%H%M%S")
        self.stats_output_dir = os.path.join(os.getcwd(), f'{run_name}_{self.timestamp}')
        self.config = config_dict
        self.pid = None
        self.subprocess_p = None
        self.psutil_p = None

    def already_run(self):
        """Will check if the run has an exitcode file. If it does, it has already been run."""
        logging.info(f'Checking if run {self.name} has already been run.')
        pass

    def cleanup_output(self):
        """Remove all the fastq and such, but keep the exitcode file"""
        logging.info(f'Cleaning up output for run {self.name}')
        pass

    def run_it(self):
        """Triggers the command to run and iteratively records the stats."""
        logging.info(f'Running run {self.name}')

        self.subp_p = subprocess.Popen(CMD.split(' '))
        self.pid = self.subp_p.pid

        self.psutil_p = psutil.Process(self.pid)

        stats = []
        iteration = 0
        zombie_found = False
        while True:
            # Collect system wide metrics
            stats.append(self._collect_stats_iteration())

            if (iteration != 0) and (iteration % 2 == 0):
                logging.info(f'Run {self.name} still running after {iteration} iterations.')

            # Sleep so that we don't collect too many stats
            time.sleep(args.sampling_rate)
            iteration += 1

            # Check if iteration should end
            returncode = self.subp_p.poll()
            if returncode is not None or zombie_found:
                print(f'Process has terminated with returncode {returncode}')
                exitcode_file = os.path.join(self.stats_output_dir, 'exitcode.txt')
                with open(exitcode_file, 'w') as f:
                    f.write(str(returncode))
                print(stats)
                break

    def _collect_stats_iteration(self):
        stats_d = {}
        try:
            with self.psutil_p.oneshot():
                stats_d['system_cpu_percent'] = psutil.cpu_percent()
                stats_d['system_memory_full_info'] = psutil.virtual_memory()

            # Collect process specific metrics
            with self.psutil_p.oneshot():
                stats_d['process_cpu_percent'] = self.psutil_p.cpu_percent()
        # Warnings and not an error since these seem to happen when the process has been finished
        except psutil.ZombieProcess:
            logging.warning(f'Process {self.pid} is a zombie.')
        except psutil.NoSuchProcess:
            logging.warning(f'Process {self.pid} does not exist. (probably exited)')
        return stats_d


def main(args):
    # Read in the yaml config
    with open(args.run_parameter_file, 'r') as conf_fh:
        config = yaml.safe_load(conf_fh)

    runs = config['runs']

    # For each run
    for run_d in runs:
        run = Run(run_d['name'], run_d)
        # Check if it has already been run
        if run.already_run():
            # cleanup the old run
            run.cleanup_output()
            continue

        # Run it and wait for it to finish
        run.run_it()
        # clean up the old run
        run.cleanup_output()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input_dir', help='The directory containing the bcl files.')
    parser.add_argument('run_parameter_file', help='The file containing the run parameters.')
    parser.add_argument('output_dir', help='The directory to output the stats and fastq files to.')
    parser.add_argument('--sampling_rate', type=int, default=1, help='The sampling rate in seconds.')
    args = parser.parse_args()
    main(args)