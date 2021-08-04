import os
import sys
import yaml
from pathlib import Path
import colorama
from colorama import Fore
from colorama import Style
from statistics import mean

NUM_TRIALS = 5
NUM_RETRIES = 0


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
    print(Fore.BLUE + Style.BRIGHT, prepend, " [WARN]: ", Style.RESET_ALL, *args, file=sys.stderr)


def mean_func(arr):
    return mean(arr)


def median_func(arr):
    return arr[int(len(arr) * 0.50)]


def p99_func(arr):
    return arr[int(len(arr) * 0.99)]


def p999_func(arr):
    return arr[int(len(arr) * 0.999)]


def parse_latency_log(log, threshold):
    if not (os.path.exists(log)):
        warn("Path {} does not exist".format(log))
        return []
    with open(log) as f:
        raw_lines = f.readlines()
        lines = [int(line.strip()) for line in raw_lines]
    front_cutoff = int(len(lines) * threshold)
    end_cutoff = int(len(lines) * (1.0 - threshold))
    lines = lines[front_cutoff:end_cutoff]
    return sorted(lines)


def get_tput_gbps(pkts_per_sec, pkt_size):
    return float(pkts_per_sec) * float(pkt_size) * 8.0 / float(1e9)


def get_tput_pps(tput_gbps, pkt_size):
    return tput_gbps * float(1e9) / (float(pkt_size) * 8.0)


def parse_command_line_args(log):
    if not (os.path.exists(log)):
        warn("Path {} does not exist".format(log))
        return {}
    return yaml.load(Path(log).read_text())


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
