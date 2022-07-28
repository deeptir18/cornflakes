import os
import pandas as pd
import sys
import yaml
from pathlib import Path
import colorama
from colorama import Fore
from colorama import Style
from statistics import mean
import copy
import json
import numpy as np
import torch

NUM_TRIALS = 1
NUM_RETRIES = 0

PERCENT_ACHIEVED_CUTOFF = 0.98
DEFAULT_HISTOGRAM_PRECISION = 1000

class Histogram(object):
    def __init__(self, histogram_yaml_map):
        if "precision" in histogram_yaml_map and "map" in histogram_yaml_map and "count" in histogram_yaml_map:

            self._precision = int(histogram_yaml_map["precision"])
            self._histogram = {int(k): int(v) for k,v in
                    histogram_yaml_map["map"].items()}
            self._count = int(histogram_yaml_map["count"])
        else:
            self._precision = DEFAULT_HISTOGRAM_PRECISION
            self._histogram = {}
            self._count = 0
    
    @property
    def precision(self):
        return self._precision

    @property
    def histogram(self):
        return self._histogram

    @property
    def count(self):
        return self._count

    def add_latency_from_hist(self, latency, count):
        bucket = latency
        if not(latency % self._precision == 0):
            bucket = int((latency / self._precision  + 1) *
                    self._precision)
        if bucket in self._histogram:
            self._histogram[bucket] += count
        else:
            self._histogram[bucket] = count

    
    def combine(self, other):
        self._count += other.count
        if self._precision >= other.precision:
            for key, count in other.histogram.items():
                self.add_latency_from_hist(key, count)
        else:
            for key, count in self._histogram.items():
                other.add_latency_from_hist(key, count)
            self._precision = other.precision
            self._histogram = copy.deepcopy(other.histogram)
    def avg(self):
        # TODO: does this overflow?
        total = 0
        for key, count in self._histogram.items():
            total += key * count
        return total / float(self._count)
    def value_at_quantile(self, quantile):
        """
        Calculates percentile given decimal quantile.
        """
        if quantile >= 1:
            raise Exception("Quantile must be less than 1")
        index = int(self.count * quantile)
        cur_index = 0
        for key in sorted(self._histogram.keys()):
            count = self._histogram[key]
            if count == 0:
                continue
            for i in range(0, count):
                if cur_index == index:
                    return key
                cur_index += 1
        raise Exception("unreachable")





def read_threads_json(json_file, thread_id):
    f = open(json_file)
    data = json.load(f)
    thread_map = data['{}'.format(thread_id)]
    return thread_map


def info(*args):
    prepend = "\u2192"
    print(Fore.BLUE + Style.BRIGHT, prepend, "[INFO]: ", Style.RESET_ALL, *args, file=sys.stderr)


def debug(*args):
    prepend = "\u2192"
    print(Fore.GREEN + Style.BRIGHT, prepend,  "[DEBUG]: ", Style.RESET_ALL, *args, file=sys.stderr)


def error(*args):
    prepend = "\u2192"
    print(Fore.RED + Style.BRIGHT, prepend, "[ERROR]: ", Style.RESET_ALL, *args, file=sys.stderr)


def warn(*args):
    prepend = "\u2192"
    print(Fore.BLUE + Style.BRIGHT, prepend, "[WARN]: ", Style.RESET_ALL, *args, file=sys.stderr)


def mean_func(arr):
    mean = torch.mean(arr)
    return mean.item()


def median_func(arr):
    median = arr[int(len(arr) * 0.50)]
    return median.item()


def p99_func(arr):
    p99 = arr[int(len(arr) * 0.99)]
    return p99.item()


def p999_func(arr):
    p999 = arr[int(len(arr) * 0.999)]
    return p999.item()


def sort_latency_lists(arrays):
    c = torch.cat(arrays)
    c_sorted, c_ind = c.sort()
    return c_sorted


def parse_latency_log(log, threshold):
    if not (os.path.exists(log)):
        warn("Path {} does not exist".format(log))
        return []
    with open(log) as f:
        raw_lines = f.readlines()
        lines = [float(line.strip()) for line in raw_lines]
        front_cutoff = int(len(lines) * threshold)
        end_cutoff = int(len(lines) * (1.0 - threshold))
        lines = lines[front_cutoff:end_cutoff]
        np_array = np.array(lines)
        v = torch.from_numpy(np_array)
        v_sorted, v_ind = v.sort()
    return v_sorted


def get_tput_gbps(pkts_per_sec, pkt_size):
    return float(pkts_per_sec) * float(pkt_size) * 8.0 / float(1e9)


def get_tput_pps(tput_gbps, pkt_size):
    return tput_gbps * float(1e9) / (float(pkt_size) * 8.0)


def parse_command_line_args(log):
    if not (os.path.exists(log)):
        warn("Path {} does not exist".format(log))
        return {}
    return yaml.load(Path(log).read_text(), Loader=yaml.FullLoader)


def parse_number_trials_done(exp_folder):
    num_trials = 0
    if not (os.path.exists(str(exp_folder))):
        return num_trials
    for folder in os.listdir(exp_folder):
        if not(os.path.isdir(exp_folder / folder)):
            continue
        if "trial" in str(folder):
            num_trials += 1
    return num_trials


def check_log_extension(arg_value):
    if not arg_value.endswith(".log"):
        warn("Filename must end with .log")
        raise argparse.ArgumentTypeError
    return arg_value


def get_postprocess_logfile(logfile):

    new_logfile = logfile[0:len(logfile) - 4] + "-postprocess" + ".log"
    return new_logfile
