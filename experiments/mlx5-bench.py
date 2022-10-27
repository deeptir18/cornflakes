import numpy as np
from main import runner
import pandas as pd
import time
from main import utils
import heapq
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
import hashlib
import json
STRIP_THRESHOLD = 0.03

# used for array size experiment
# L1 cache = 32K, L2 = 1024K, L3 = ~14080K
COMPLETE_ARRAY_SIZES_TO_LOOP = [65536, 819200, 4096000, 65536000, 655360000]

# used for recv size experiment (just do 1 size and 1 number of segments for
# now)
COMPLETE_RECV_SIZES_TO_LOOP = [256, 512, 1024, 2048, 4096]
RECV_SIZE_SEGMENTS_TO_LOOP = [2]
RECV_SIZE_TOTAL_SIZES_TO_LOOP = [256, 4096]

# used for total size experiment
COMPLETE_TOTAL_SIZES_TO_LOOP = [32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]


# used for other experiments, which total sizes to check
TOTAL_SIZES_TO_LOOP = [256, 4096]
# TOTAL_SIZES_TO_LOOP = [4096]

# used for segment size experiment
COMPLETE_SEGMENTS_TO_LOOP = [1, 2, 4, 8, 16, 32]
# COMPLETE_SEGMENTS_TO_LOOP = [1, 2]

# used for other experiment, which segment amounts to check
SEGMENTS_TO_LOOP = [2, 8]
# SEGMENTS_TO_LOOP = [2]

NUM_THREADS = 4
NUM_CLIENTS = 3
rates = [5000, 10000, 50000, 100000, 200000,
         300000, 400000, 410000, 420000, 431000]
# max rates to get "knee" (for smallest working set size, 0 extra busy work)
# below is for without batching, 4 threads per client
max_rates = {32: 750000, 64: 600000, 128: 500000, 256: 425000,
             512: 400000, 1024: 375000, 2048: 350000, 4096: 225000, 8192: 150000}
# below is for with batching (16 packets), and 8 threads per client
#max_rates = {512: 350000, 1024: 300000, 2048: 175000}

sample_percentages = [10, 30, 40, 45, 50, 53, 55, 57, 60,
                      63, 66, 69, 72, 75, 78, 81, 83, 85, 88, 91, 93, 95, 100]


def json_dumps(thing):
    return json.dumps(
        thing,
        ensure_ascii=False,
        sort_keys=True,
        indent=None,
        separators=(',', ':'),
    )


def parse_client_time_and_pkts(line):
    fmt = parse.compile("Ran for {} seconds, sent {} packets.")
    time, pkts_sent = fmt.parse(line)
    return (time, pkts_sent)


def parse_client_pkts_received(line):
    fmt = parse.compile(
        "Num fully received: {}, packets per bucket: {}, total_count: {}.")
    received, pkts_per_bucket, total = fmt.parse(line)
    return received


def parse_log_info(log):
    if not(os.path.exists(log)):
        utils.warn("Path {} does not exist".format(log))
        return {}
    ret = {}
    with open(log) as f:
        raw_lines = f.readlines()
        lines = [line.strip() for line in raw_lines]
        for line in lines:
            if line.startswith("Ran for"):
                (time, pkts_sent) = parse_client_time_and_pkts(line)
                ret["totaltime"] = float(time)
                ret["pkts_sent"] = int(pkts_sent)
            elif line.startswith("Num fully received"):
                pkts_received = parse_client_pkts_received(line)
                ret["pkts_received"] = int(pkts_received)

        return ret


class ScatterGatherIteration(runner.Iteration):
    def __init__(self, client_rates, segment_size,
                 num_segments, with_copy, as_one, num_threads, trial=None,
                 array_size=8192, busy_cycles=0, recv_pkt_size=0, echo_mode=False, num_cores=1):
        """
        Arguments:
        * client_rates: Mapping from {int, int} specifying rates and how many
        clients send at that rate.
        Total clients cannot exceed the maximum clients on the machine.
        * segment_size: Segment size each client is sending at
        * num_segments: Number of separate scattered buffers the clients are using.
        * with_copy: Whether the server is copying out the payload.
        """
        self.client_rates = client_rates
        self.segment_size = segment_size
        self.num_segments = num_segments
        self.with_copy = with_copy
        self.num_threads = num_threads
        self.trial = trial
        self.as_one = as_one
        self.array_size = array_size
        self.busy_cycles = busy_cycles
        self.recv_pkt_size = recv_pkt_size
        self.echo_mode = echo_mode
        self.num_cores = num_cores

    def hash(self):
        args = [self.segment_size, self.num_segments, self.with_copy,
                self.num_threads, self.get_num_clients(), self.self.trial, self.as_one, self.array_size,
                self.busy_cycles, self.recv_pkt_size, self.echo_mode]
        return hashlib.md5(json_dumps(args).encode('utf-8')).hexdigest()

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment
        """
        return ["segment_size", "num_segments", "with_copy", "as_one",
        "array_size", "busy_cycles", "recv_size", "num_threads",
        "num_clients", "echo_mode", "offered_load_pps", "offered_load_gbps"]

    def get_iteration_params_values(self):
        """
        Returns dictionary of above iteration params.
        """
        offered_load_pps = 0
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            offered_load_pps += rate * num * self.num_threads
        # convert to gbps
        offered_load_gbps = utils.get_tput_gbps(offered_load_pps,
                self.segment_size * self.num_segments)

        return {
                "segment_size": self.segment_size,
                "num_segments": self.num_segments,
                "with_copy": self.with_copy
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "echo_mode": self.echo_mode,
                "offered_load_pps": offered_load_pps,
                "offered_load_gbps": offered_load_gbps,
        }
    def get_iteration_avg_message_size(self):
        return self.segment_size * self.num_segments

    def get_echo_mode(self):
        return self.echo_mode

    def get_num_cores(self):
        return self.num_cores
        
    def get_busy_cycles(self):
        return self.busy_cycles

    def get_num_threads(self):
        return self.num_threads

    def get_num_clients(self):
        ret = 0
        for info in self.client_rates:
            ret += info[1]
        return ret

    def get_array_size(self):
        return self.array_size

    def get_recv_pkt_size(self):
        return self.recv_pkt_size

    def get_segment_size(self):
        return self.segment_size

    def get_num_segments(self):
        return self.num_segments

    def get_with_copy(self):
        return self.with_copy

    def get_as_one(self):
        return self.as_one

    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_total_size(self):
        return self.num_segments * self.segment_size

    def get_num_cores_string(self):
        return "server_cores_{:d}".format(self.num_cores)
    
    def get_client_rate_string(self):
        # 2@300000,1@100000 implies 2 clients at 300000 pkts / sec and 1 at
        # 100000 pkts / sec
        ret = ""
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            if ret != "":
                ret += ","
            ret += "{}@{}".format(num, rate)
        return ret

    def get_host_list(self, host_type_map):
        ret = []
        if "server" in host_type_map:
            ret.extend(host_type_map["server"])
        if "client" in host_type_map:
            ret.extend(self.get_iteration_clients(host_type_map["client"]))
        return ret

    def get_program_hosts(self, program_name, host_type_map):
        ret = []
        if program_name == "start_server":
            return host_type_map["server"]
        elif program_name == "start_client":
            return self.get_iteration_clients(host_type_map["client"]) 

    def get_iteration_clients(self, possible_hosts):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return possible_hosts[0:total_hosts]

    def find_rate(self, client_options, host):
        rates = []
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            for idx in range(num):
                rates.append(rate)
        try:
            rate_idx = client_options.index(host)
            return rates[rate_idx]
        except:
            utils.error("Host {} not found in client options {}.".format(
                        host,
                        client_options))
            exit(1)

    def get_recv_pkt_size_string(self):
        if self.recv_pkt_size == 0:
            return "recv_size_small"
        else:
            return "recv_size_{}".format(self.recv_pkt_size)

    def get_with_echo_string(self):
        if self.echo_mode:
            return "echo_on"
        else:
            return "echo_off"

    def get_with_copy_string(self):
        if self.with_copy:
            if self.as_one:
                return "with_copy_one_buffer"
            else:
                return "with_copy"
        else:
            if self.as_one:
                return "zero_copy_one_buffer"
            else:
                return "zero_copy"

    def get_busy_cycles_string(self):
        return "busycycles_{}".format(self.busy_cycles)

    def get_num_threads_string(self):
        return "threads_{}".format(self.num_threads)

    def get_array_size_string(self):
        return "arraysize_{}".format(self.array_size)

    def get_segment_size_string(self):
        return "segmentsize_{}".format(self.segment_size)

    def get_num_segments_string(self):
        # if with_copy is turned on, server copies in segment size increments
        return "mbufs_{}".format(self.num_segments)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def __str__(self):
        return "Iteration info: client rates: {}, "\
            "segment size: {}, "\
            " num_segments: {}, "\
            " with_copy: {},"\
            "echo: {},"\
            "array size: {},"\
            "busy cycles us: {},"\
            "num client threads: {},"\
            "recv pkt size: {},"\
            " trial: {}".format(self.get_client_rate_string(),
                                self.get_segment_size_string(),
                                self.get_num_segments_string(),
                                self.get_with_copy_string(),
                                self.get_with_echo_string(),
                                self.get_array_size(),
                                self.get_busy_cycles(),
                                self.get_num_threads(),
                                self.get_recv_pkt_size(),
                                self.get_trial_string())

    def get_size_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.get_segment_size_string()

    def get_parent_folder(self, high_level_folder):
        # returned path doesn't include the trial
        path = Path(high_level_folder)
        return path / self.get_segment_size_string() /\
            self.get_num_segments_string() / self.get_array_size_string() /\
            self.get_recv_pkt_size_string() /\
            self.get_busy_cycles_string() /\
            self.get_client_rate_string() / self.get_num_threads_string() / \
            self.get_with_copy_string() / \
            self.get_num_cores_string()

    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def find_client_id(self, host):
        return int(host[len(host) - 1])

    def get_num_clients(self):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return total_hosts

    def get_program_args(self,
                         folder,
                         program,
                         host,
                         config_yaml,
                         programs_metadata,
                         exp_time):
        ret = {}
        ret["config_eal"] = " ".join(config_yaml["dpdk"]["eal_init"])
        ret["pci_addr"] = config_yaml["dpdk"]["pci_addr"]
        ret["array_size"] = self.array_size
        ret["num_threads"] = self.num_threads
        ret["num_machines"] = self.get_num_clients()
        ret["random_seed"] = int(time.time())
        ret["busy_cycles"] = self.busy_cycles
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        # both sides need to know about the server mac address
        ret["server_mac"] = config_yaml["hosts"][server_host]["mac"]
        ret["server_ip"] = config_yaml["hosts"][server_host]["ip"]
        # set with_copy, segment_size, num_segments based on if it is with_copy
        if self.with_copy:
            ret["with_copy"] = " --with_copy"
            if self.as_one:
                ret["as_one"] = "as_one"
                ret["segment_size"] = self.segment_size * self.num_segments
                ret["num_segments"] = 1
            else:
                ret["segment_size"] = self.segment_size
                ret["num_segments"] = self.num_segments
        else:
            ret["with_copy"] = ""
            if self.as_one:
                ret["as_one"] = "as_one"
                ret["num_segments"] = 1
                ret["segment_size"] = self.segment_size * self.num_segments
            else:
                ret["segment_size"] = self.segment_size
                ret["num_segments"] = self.num_segments
        ret["cornflakes_dir"] = config_yaml["cornflakes_dir"]
        ret["top_dir"] = config_yaml["top_dir"]
        ret["folder"] = str(folder)
        if (self.echo_mode):
            ret["echo_str"] = " --echo_mode"
        else:
            ret["echo_str"] = ""
        if program == "start_server":
            if (self.recv_pkt_size != 0):
                ret["read_pkt_str"] = " --read_incoming_packet"
            else:
                ret["read_pkt_str"] = ""
            ret['num_cores'] = "{:d}".format(self.num_cores)
        elif program == "start_client":
            if (self.recv_pkt_size != 0):
                ret["send_packet_size_str"] = " --has_send_packet_size --send_packet_size={}".format(
                    self.recv_pkt_size)
            else:
                ret["send_packet_size_str"] = ""
            # calculate client rate
            host_options = self.get_iteration_clients(
                programs_metadata[program]["hosts"])
            rate = self.find_rate(host_options, host)
            ret["host_id"] = self.find_client_id(host) - 1
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
            ret["rate"] = rate
            ret["time"] = exp_time
            ret["latency_log"] = "{}.latency.log".format(host)
            ret["host"] = host
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret


class ScatterGather(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "ScatterGather"
        self.config_yaml = yaml.load(Path(config_yaml).read_text(),
                Loader=yaml.FullLoader)
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text(),
                Loader=yaml.FullLoader)
        # map from (iteration with rate set to 0, (throughput, percent achieved, stop))
        self.iteration_skipping_information = {}

    def experiment_name(self):
        return self.exp

    def append_to_skip_info(self, total_args, iteration, higher_level_folder):
        if total_args.exp_type == "individual":
            return
        if total_args.looping_variable != "total_segment_cross":
            return
        else:
            iteration_hash = iteration.hash()
            try:
                throughput = iteration.read_key_from_analysis_log(higher_level_folder, "achieved_load_gbps")
                percent_achieved = iteration.read_key_from_analysis_log(higher_level_folder,
                        "percent_achieved_rate")
            except:
                utils.warn("Could not read throughput and percent achieved for trial {}".format(str(iteration)))
                continue
            if iteration_hash not in self.iteration_skipping_information:
                self.iteration_skipping_information[iteration_hash] = (
                    throughput, percent_achieved, False)
                if (throughput == 0 and percent_achieved == 0):
                    return
            else:
                current_highest_throughput, current_percent_achieved, current_stop = self.iteration_skipping_information[
                    iteration_hash]
                # if previous iteration does not meet percent achieved cutoff
                if current_percent_achieved < utils.PERCENT_ACHIEVED_CUTOFF:
                    self.iteration_skipping_information[iteration_hash] = (
                        throughput, percent_achieved, False)
                elif current_highest_throughput > throughput:
                    self.iteration_skipping_information[iteration_hash] = (
                        throughput, percent_achieved, True)

    def skip_iteration(self, total_args, iteration):
        if total_args.exp_type == "individual":
            return False
        if total_args.looping_variable != "total_segment_cross":
            return False
        iteration_hash = iteration.hash()
        if iteration_hash not in self.iteration_skipping_information:
            return False
        else:
            throughput, percent_achieved, skip = self.iteration_skipping_information[
                iteration_hash]
            return skip

    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > self.config_yaml["max_clients"]:
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)
            client_rates = [(total_args.rate, total_args.num_clients)]
            it = ScatterGatherIteration(client_rates,
                                        total_args.segment_size,
                                        total_args.num_segments,
                                        total_args.with_copy,
                                        total_args.as_one,
                                        total_args.num_threads,
                                        array_size=total_args.array_size,
                                        busy_cycles=total_args.busy_cycles,
                                        recv_pkt_size=total_args.recv_size,
                                        echo_mode=total_args.echo,
                                        num_cores=total_args.num_cores)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            it.set_trial(num_trials_finished)
            return [it]
        else:
            ret = []
            if total_args.looping_variable == "total_segment_cross":
                for trial in range(utils.NUM_TRIALS):
                    array_size = 65536
                    for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                        max_rate = max_rates[total_size]
                        for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                            for with_copy in [False, True]:
                                for sampling in reversed(sample_percentages):
                                    rate = int(float(sampling/100) *
                                               max_rate)
                                    segment_size = int(
                                        total_size / num_segments)
                                    as_one = False

                                    it = ScatterGatherIteration([(rate,
                                                                  NUM_CLIENTS)],
                                                                segment_size,
                                                                num_segments,
                                                                with_copy,
                                                                as_one,
                                                                NUM_THREADS,
                                                                trial=trial,
                                                                array_size=array_size)
                                    ret.append(it)

            elif total_args.looping_variable == "recv_size":
                for trial in range(utils.NUM_TRIALS):
                    array_size = 65536
                    for total_size in RECV_SIZE_TOTAL_SIZES_TO_LOOP:
                        for recv_size in COMPLETE_RECV_SIZES_TO_LOOP:
                            max_rate = max_rates[total_size]
                            for sampling in sample_percentages:
                                rate = int(float(sampling/100) *
                                           max_rate)
                                for with_copy in [False, True]:
                                    for num_segments in RECV_SIZE_SEGMENTS_TO_LOOP:
                                        segment_size = int(
                                            total_size / num_segments)
                                        as_one = False
                                        it = ScatterGatherIteration([(rate,
                                                                      NUM_CLIENTS)],
                                                                    segment_size,
                                                                    num_segments,
                                                                    with_copy,
                                                                    as_one,
                                                                    NUM_THREADS,
                                                                    trial=trial,
                                                                    array_size=array_size,
                                                                    recv_pkt_size=recv_size)
                                        ret.append(it)
            elif total_args.looping_variable == "total_size":
                for trial in range(utils.NUM_TRIALS):
                    array_size = 65536
                    for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                        max_rate = max_rates[total_size]
                        for sampling in sample_percentages:
                            rate = int(float(sampling/100) *
                                       max_rate)
                            for with_copy in [False, True]:
                                for num_segments in SEGMENTS_TO_LOOP:
                                    segment_size = int(
                                        total_size / num_segments)
                                    as_one = false
                                    it = scattergatheriteration([(rate,
                                                                 num_clients)],
                                                                segment_size,
                                                                num_segments,
                                                                with_copy,
                                                                as_one,
                                                                num_threads,
                                                                trial=trial,
                                                                array_size=array_size)
                                    ret.append(it)
            elif total_args.looping_variable == "num_segments":
                for trial in range(utils.NUM_TRIALS):
                    array_size = 65536
                    for total_size in TOTAL_SIZES_TO_LOOP:
                        max_rate = max_rates[total_size]
                        # sample 10 rates between minimum and max rate,
                        # but sample more towards the max rate
                        for sampling in sample_percentages:
                            rate = int(float(sampling/100) *
                                       max_rate)
                            for with_copy in [False, True]:
                                for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                                    segment_size = int(
                                        total_size / num_segments)
                                    as_one = False
                                    it = ScatterGatherIteration([(rate,
                                                                 NUM_CLIENTS)],
                                                                segment_size,
                                                                num_segments,
                                                                with_copy,
                                                                as_one,
                                                                NUM_THREADS,
                                                                trial=trial,
                                                                array_size=array_size)
                                    ret.append(it)
            elif total_args.looping_variable == "array_total_size":
                for trial in range(utils.NUM_TRIALS):
                    for array_size in COMPLETE_ARRAY_SIZES_TO_LOOP:
                        for total_size in TOTAL_SIZES_TO_LOOP:
                            max_rate = max_rates[total_size]
                            # sample 10 rates between minimum and max rate,
                            # but sample more towards the max rate
                            for sampling in sample_percentages:
                                rate = int(float(sampling/100) *
                                           max_rate)
                                for with_copy in [False, True]:
                                    for num_segments in SEGMENTS_TO_LOOP:
                                        segment_size = int(
                                            total_size / num_segments)
                                        as_one = False
                                        it = ScatterGatherIteration([(rate,
                                                                     NUM_CLIENTS)],
                                                                    segment_size,
                                                                    num_segments,
                                                                    with_copy,
                                                                    as_one,
                                                                    NUM_THREADS,
                                                                    trial=trial,
                                                                    array_size=array_size)
                                        ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-wc", "--with_copy",
                            dest="with_copy",
                            action='store_true',
                            help="Whether the server uses a copy or not.")
        parser.add_argument("-o", "--as_one",
                            dest="as_one",
                            action='store_true',
                            help="Whether the server sends the payload as a single buffer.")
        parser.add_argument("-arr", "--array_size",
                            dest="array_size",
                            type=int,
                            default=10000,
                            help="Array size")
        parser.add_argument("-echo", "--echo",
                            dest="echo",
                            action='store_true',
                            help="Whether the server should use zero-copy echo mode or not.")
        parser.add_argument("-rs", "--recv_size",
                            dest="recv_size",
                            type=int,
                            default=0,
                            help="If set, receive packet size")
        parser.add_argument("-bc", "--busy_cycles",
                            dest="busy_cycles",
                            type=int,
                            default=0,
                            help="Busy cycles in us")
        parser.add_argument("-k", "--num_cores",
                            dest="num_cores",
                            type=int,
                            default=1,
                            help="Number of server cores")
        if namespace.exp_type == "individual":
            parser.add_argument("-r", "--rate",
                                dest="rate",
                                type=int,
                                default=300000,
                                help="Rate of client(s) (pkts/s).")
            parser.add_argument("-s", "--segment_size",
                                help="Size of segment",
                                type=int,
                                default=512)
            parser.add_argument("-m", "--num_segments",
                                help="Number of segments",
                                type=int,
                                default=1)
            parser.add_argument('-nc', "--num_clients",
                                help="Number of clients",
                                type=int,
                                default=1)
            parser.add_argument("-t", "--num_threads",
                                help="Number of threads per client",
                                type=int,
                                default=1)
        else:
            parser.add_argument("-l", "--logfile",
                                help="Logfile name",
                                type=utils.check_log_extension,
                                default="summary.log")
            parser.add_argument("-lp", "--looping_variable",
                                dest="looping_variable",
                                choices=["array_total_size", "total_size",
                                         "num_segments", "recv_size",
                                         "total_segment_cross", "num_cores"],
                                default="array_total_size",
                                help="What variable to loop over")
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "segment_size,num_segments,with_copy,as_one,array_size,busy_cycles,recv_size," \
            "num_threads,num_clients,offered_load_pps,offered_load_gbps," \
            "achieved_load_pps,achieved_load_gbps," \
            "percent_achieved_rate," \
            "avg,median,p99,p999"

    def run_summary_analysis(self, df, out, array_size, recv_size, num_segments, segment_size, with_copy):
        filtered_df = df[(df.array_size == array_size) &
                         (df.recv_size == recv_size) &
                         (df.num_segments == num_segments) &
                         (df.segment_size == segment_size) &
                         (df.with_copy == with_copy)]
        # calculate lowest rate, get p99 and median
        # stats
        min_rate = filtered_df["offered_load_pps"].min()
        latency_df = filtered_df[(filtered_df.offered_load_pps == min_rate)]
        p99_mean = latency_df["p99"].mean()
        p99_sd = latency_df["p99"].std(ddof=0)
        median_mean = latency_df["median"].mean()
        median_sd = latency_df["median"].std(ddof=0)
        # filtered_df = filtered_df[filtered_df["percent_achieved_rate"] >= .95]

        def ourstd(x):
            return np.std(x, ddof=0)

        # CURRENT KNEE CALCULATION:
        # just find maximum achieved rate across all rates
        # group by array size, num segments, segment size,  # average
        clustered_df = filtered_df.groupby(["array_size",
                                            "num_segments", "segment_size", "with_copy",
                                            "recv_size",
                                           "offered_load_pps",
                                            "offered_load_gbps"],
                                           as_index=False).agg(
            achieved_load_pps_mean=pd.NamedAgg(column="achieved_load_pps",
                                               aggfunc="mean"),
            achieved_load_pps_sd=pd.NamedAgg(column="achieved_load_pps",
                                             aggfunc=ourstd),
            percent_achieved_rate=pd.NamedAgg(column="percent_achieved_rate",
                                              aggfunc='mean'),
            achieved_load_gbps_mean=pd.NamedAgg(column="achieved_load_gbps",
                                                aggfunc="mean"),
            achieved_load_gbps_sd=pd.NamedAgg(column="achieved_load_gbps",
                                              aggfunc=ourstd))
        # print("before filtering: ", clustered_df)
        # clustered_df = clustered_df[clustered_df["percent_achieved_rate"] >=
        #                            .95]
        # print("after filtering: ", clustered_df)
        # print("idx max: ", clustered_df["achieved_load_pps_mean"].idxmax())
        # print("not getting here")

        max_achieved_pps = clustered_df["achieved_load_pps_mean"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_mean"].max()
        std_achieved_pps = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'achieved_load_pps_sd']
        std_achieved_gbps = clustered_df.loc[clustered_df['achieved_load_gbps_mean'].idxmax(),
                                             'achieved_load_gbps_sd']
        as_one = False
        out.write(str(array_size) + "," + str(segment_size) + "," +
                  str(num_segments) + "," + str(recv_size) +
                  "," + str(with_copy) + "," +
                  str(as_one) + "," + str(p99_mean) + "," +
                  str(p99_sd) + "," + str(median_mean) +
                  "," + str(median_sd) + "," +
                  str(max_achieved_pps) + "," +
                  str(max_achieved_gbps) + "," +
                  str(std_achieved_pps) + "," +
                  str(std_achieved_gbps) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        # need to determine summary "p99 at low rate, median at low rate, knee
        # of the curve" for each situation
        utils.info("Running post process analysis")
        header_str = "array_size,segment_size,num_segments,recv_size,with_copy,as_one,mp99,p99sd,mmedian,mediansd,maxtputpps,maxtputgbps,maxtputppssd,maxtputgbpssd" + os.linesep

        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)

        if total_args.looping_variable == "total_segment_cross":
            array_size = 65536
            recv_size = 0
            for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                    segment_size = int(total_size / num_segments)
                    for with_copy in [False, True]:
                        self.run_summary_analysis(df, out,
                                                  array_size, recv_size,
                                                  num_segments, segment_size, with_copy)
        elif total_args.looping_variable == "recv_size":
            array_size = 65536
            for total_size in RECV_SIZE_TOTAL_SIZES_TO_LOOP:
                for recv_size in COMPLETE_RECV_SIZES_TO_LOOP:
                    for num_segments in RECV_SIZE_SEGMENTS_TO_LOOP:
                        segment_size = int(total_size / num_segments)
                        for with_copy in [False, True]:
                            self.run_summary_analysis(df, out,
                                                      array_size, recv_size,
                                                      num_segments, segment_size, with_copy)

        elif total_args.looping_variable == "total_size":
            array_size = 65536
            recv_size = 0
            for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                for num_segments in SEGMENTS_TO_LOOP:
                    segment_size = int(total_size / num_segments)
                    for with_copy in [False, True]:
                        self.run_summary_analysis(df, out,
                                                  array_size, recv_size,
                                                  num_segments, segment_size, with_copy)
        elif total_args.looping_variable == "num_segments":
            array_size = 65536
            recv_size = 0
            for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                for total_size in TOTAL_SIZES_TO_LOOP:
                    segment_size = int(total_size / num_segments)
                    for with_copy in [False, True]:
                        self.run_summary_analysis(df, out,
                                                  array_size, recv_size,
                                                  num_segments, segment_size, with_copy)

        elif total_args.looping_variable == "array_total_size":
            recv_size = 0
            df["recv_size"] = recv_size  # for the data we ran with
            for array_size in COMPLETE_ARRAY_SIZES_TO_LOOP:
                for total_size in TOTAL_SIZES_TO_LOOP:
                    for num_segments in SEGMENTS_TO_LOOP:
                        segment_size = int(total_size / num_segments)
                        for with_copy in [False, True]:
                            self.run_summary_analysis(df, out,
                                                      array_size, recv_size,
                                                      num_segments, segment_size, with_copy)
        out.close()

    def run_plot_cmd(self, args):
        try:
            print(" ".join(args))
            sh.run(args)
        except:
            utils.warn(
                "Failed to run plot command: {}".format(args))
            exit(1)

    def run_analysis_individual_trial(self,
                                      higher_level_folder,
                                      program_metadata,
                                      iteration,
                                      print_stats=False):
        exp_folder = iteration.get_folder_name(higher_level_folder)
        # parse each client log
        # parse stdout logs
        total_offered_load_pps = 0
        total_offered_load_gbps = 0
        total_achieved_load_gbps = 0
        total_achieved_load_pps = 0
        total_retries = 0
        client_latency_lists = []
        clients = iteration.get_iteration_clients(
            program_metadata["start_client"]["hosts"])

        num_threads = iteration.get_num_threads()
        num_clients = iteration.get_num_clients()

        for host in clients:
            args = {"folder": str(exp_folder), "host": host}
            thread_file = "{folder}/{host}.threads.log".format(**args)
            for thread in range(iteration.get_num_threads()):
                args["thread"] = thread  # replace thread number
                latency_log = "{folder}/{host}.latency-t{thread}.log".format(
                    **args)
                latencies = utils.parse_latency_log(latency_log,
                                                    STRIP_THRESHOLD)
                if len(latencies) == 0:
                    utils.warn(
                        "Error parsing latency log {}".format(latency_log))
                    return ""
                client_latency_lists.append(latencies)

                thread_info = utils.read_threads_json(thread_file, thread)

                host_offered_load_pps = float(thread_info["offered_load_pps"])
                host_offered_load_gbps = float(
                    thread_info["offered_load_gbps"])
                total_offered_load_pps += host_offered_load_pps
                total_offered_load_gbps += host_offered_load_gbps

                host_achieved_load_pps = float(
                    thread_info["achieved_load_pps"])
                host_achieved_load_gbps = float(
                    thread_info["achieved_load_gbps"])
                total_achieved_load_pps += host_achieved_load_pps
                total_achieved_load_gbps += host_achieved_load_gbps

                # add retries
                retries = int(thread_info["retries"])
                total_retries += retries

                if print_stats:
                    # convert to microseconds
                    host_p99 = utils.p99_func(latencies) / 1000.0
                    host_p999 = utils.p999_func(latencies) / 1000.0
                    host_median = utils.median_func(latencies) / 1000.0
                    host_avg = utils.mean_func(latencies) / 1000.0
                    utils.info("Client {}, Thread {}: "
                               "offered load: {:.4f} req/s | {:.4f} Gbps, "
                               "achieved load: {:.4f} req/s | {:.4f} Gbps, "
                               "percentage achieved rate: {:.4f}, "
                               "retries: {}, "
                               "avg latency: {: .4f} \u03BCs, p99: {: .4f} \u03BCs, p999:"
                               "{: .4f} \u03BCs, median: {: .4f} \u03BCs".format(
                                   host, thread, host_offered_load_pps, host_offered_load_gbps,
                                   host_achieved_load_pps, host_achieved_load_gbps,
                                   float(host_achieved_load_pps /
                                         host_offered_load_pps),
                                   retries,
                                   host_avg, host_p99, host_p999, host_median))

        sorted_latencies = utils.sort_latency_lists(client_latency_lists)
        median = utils.median_func(sorted_latencies) / float(1000)
        p99 = utils.p99_func(sorted_latencies) / float(1000)
        p999 = utils.p999_func(sorted_latencies) / float(1000)
        avg = utils.mean_func(sorted_latencies) / float(1000)

        if print_stats:
            total_stats = "offered load: {:.4f} req/s | {:.4f} Gbps, "\
                "achieved load: {:.4f} req/s | {:.4f} Gbps, "\
                "percentage achieved rate: {:.4f},"\
                "retries: {}, "\
                "avg latency: {:.4f} \u03BCs, p99: {:.4f} \u03BCs, p999: {:.4f}"\
                "\u03BCs, median: {:.4f} \u03BCs".format(
                    total_offered_load_pps, total_offered_load_gbps,
                    total_achieved_load_pps, total_achieved_load_gbps,
                    float(total_achieved_load_pps / total_offered_load_pps),
                    total_retries,
                    avg, p99, p999, median)
            utils.info("Total Stats: ", total_stats)
        percent_acheived_load = float(total_achieved_load_pps /
                                      total_offered_load_pps)

        csv_line = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(
            iteration.get_num_cores(),
            iteration.get_segment_size(),
            iteration.get_num_segments(),
            iteration.get_with_copy(),
            iteration.get_as_one(),
            iteration.get_array_size(),
            iteration.get_busy_cycles(),
            iteration.get_recv_pkt_size(),
            iteration.get_num_threads(),
            iteration.get_num_clients(),
            total_offered_load_pps,
            total_offered_load_gbps,
            total_achieved_load_pps,
            total_achieved_load_gbps,
            percent_acheived_load,
            avg * 1000,
            median * 1000,
            p99 * 1000,
            p999 * 1000)
        return csv_line

    def graph_results(self, total_args, folder, logfile,
                      post_process_logfile):
        factor_name = total_args.looping_variable
        if total_args.looping_variable == "array_total_size":
            factor_name = "array_size"
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        post_process_log = Path(folder) / post_process_logfile

        plotting_script = Path(cornflakes_repo) /\
            "experiments" / "plotting_scripts" / "sg_bench.R"

        metrics = ["median", "p99", "tput"]
        # metrics = ["tput"]

        if factor_name == "array_size" or factor_name == "recv_size":
            # run SUMMARY
            for metric in metrics:
                output_file = plot_path /\
                    "summary_{}_{}.pdf".format(factor_name, metric)
                args = [str(plotting_script), str(full_log), str(post_process_log), str(output_file),
                        metric, "full", factor_name]
                self.run_plot_cmd(args)
        if total_args.looping_variable == "total_segment_cross":
            for metric in metrics:
                # for each set of num segments, do a size plot
                for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                    factor_name = "total_size"
                    individual_plot_path = plot_path /\
                        "numsegments_{}".format(num_segments)
                    individual_plot_path.mkdir(
                        parents=True, exist_ok=True)
                    metric_name = metric
                    if metric_name == "tput":
                        metric_name = "tput_gbps"
                    pdf = individual_plot_path /\
                        "numsegments_{}_{}.pdf".format(num_segments, metric)
                    total_plot_args = [str(plotting_script), str(full_log),
                                       str(post_process_log), str(pdf),
                                       metric_name, "individual", factor_name,
                                       "foo", str(num_segments)]
                    self.run_plot_cmd(total_plot_args)
                # make a num segments plot for each size
                for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                    factor_name = "num_segments"
                    individual_plot_path = plot_path /\
                        "totalsize_{}".format(total_size)

                    individual_plot_path.mkdir(
                        parents=True, exist_ok=True)

                    pdf = individual_plot_path /\
                        "totalsize_{}_{}.pdf".format(total_size,
                                                     metric)

                    metric_name = metric
                    if metric_name == "tput":
                        metric_name = "tput_gbps"
                    total_plot_args = [str(plotting_script), str(full_log),
                                       str(post_process_log), str(pdf),
                                       metric_name, "individual", factor_name,
                                       str(total_size), "foo"]
                    self.run_plot_cmd(total_plot_args)
                # run throughput latency curves
                for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                    for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                        if metric == "tput":
                            continue
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "numsegments_{}".format(num_segments) /\
                            "totalsize_{}".format(total_size)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "total_size_{}_numsegments_{}_{}.pdf".format(
                                total_size, num_segments, metric)
                        total_plot_args = [str(plotting_script), str(full_log),
                                           str(post_process_log), str(pdf),
                                           metric, "tput_latency", factor_name,
                                           str(total_size), str(num_segments)]
                        self.run_plot_cmd(total_plot_args)
                # run heatmap (summary) plot
                pdf = plot_path /\
                    "heatmap_{}.pdf".format(metric)

                total_plot_args = [str(plotting_script), str(full_log),
                                   str(post_process_log), str(
                                       pdf), metric, "heatmap"]

        elif total_args.looping_variable == "total_size":
            for metric in metrics:
                for num_segments in SEGMENTS_TO_LOOP:
                    individual_plot_path = plot_path /\
                        "numsegments_{}".format(num_segments)
                    individual_plot_path.mkdir(
                        parents=True, exist_ok=True)

                    metric_name = metric
                    if metric_name == "tput":
                        metric_name = "tput_gbps"
                    pdf = individual_plot_path /\
                        "numsegments_{}_{}.pdf".format(num_segments, metric)
                    total_plot_args = [str(plotting_script), str(full_log),
                                       str(post_process_log), str(pdf),
                                       metric_name, "individual", factor_name,
                                       "foo", str(num_segments)]
                    self.run_plot_cmd(total_plot_args)

                    for total_size in COMPLETE_TOTAL_SIZES_TO_LOOP:
                        if metric == "tput":
                            continue
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "numsegments_{}".format(num_segments) /\
                            "totalsize_{}".format(total_size)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "total_size_{}_numsegments_{}_{}.pdf".format(
                                total_size, num_segments, metric)
                        total_plot_args = [str(plotting_script), str(full_log),
                                           str(post_process_log), str(pdf),
                                           metric, "tput_latency", factor_name,
                                           str(total_size), str(num_segments)]
                        self.run_plot_cmd(total_plot_args)

        elif total_args.looping_variable == "num_segments":
            for metric in metrics:
                for total_size in TOTAL_SIZES_TO_LOOP:
                    individual_plot_path = plot_path /\
                        "totalsize_{}".format(total_size)

                    individual_plot_path.mkdir(
                        parents=True, exist_ok=True)

                    pdf = individual_plot_path /\
                        "totalsize_{}_{}.pdf".format(total_size,
                                                     metric)

                    metric_name = metric
                    if metric_name == "tput":
                        metric_name = "tput_gbps"
                    total_plot_args = [str(plotting_script), str(full_log),
                                       str(post_process_log), str(pdf),
                                       metric_name, "individual", factor_name,
                                       str(total_size), "foo"]
                    self.run_plot_cmd(total_plot_args)

                    for num_segments in COMPLETE_SEGMENTS_TO_LOOP:
                        if metric == "tput":
                            continue
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "totalsize_{}".format(total_size) /\
                            "numsegments_{}".format(num_segments)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "totalsize_{}_numsegments_{}_{}.pdf".format(total_size,
                                                                        num_segments, metric)
                        total_plot_args = [str(plotting_script), str(full_log),
                                           str(post_process_log), str(pdf),
                                           metric, "tput_latency", factor_name, str(
                            total_size),
                            str(num_segments)]
                        self.run_plot_cmd(total_plot_args)

        elif total_args.looping_variable == "recv_size":
            for metric in metrics:
                for total_size in RECV_SIZE_TOTAL_SIZES_TO_LOOP:
                    for num_segments in RECV_SIZE_SEGMENTS_TO_LOOP:
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "totalsize_{}".format(total_size) /\
                            "numsegments_{}".format(num_segments)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "totalsize_{}_numsegments_{}_{}.pdf".format(total_size,
                                                                        num_segments, metric)

                        metric_name = metric
                        if metric_name == "tput":
                            metric_name = "tput_pps"
                        total_plot_args = [str(plotting_script), str(full_log),
                                           str(post_process_log), str(pdf),
                                           metric_name, "individual", factor_name, str(
                            total_size),
                            str(num_segments)]
                        self.run_plot_cmd(total_plot_args)

                        # tput latency for each recv size
                        for recv_size in COMPLETE_RECV_SIZES_TO_LOOP:
                            if metric == "tput":
                                continue
                            individual_plot_path = plot_path /\
                                "totalsize_{}".format(total_size) /\
                                "numsegments_{}".format(num_segments) /\
                                "recvsize_{}".format(recv_size)
                            individual_plot_path.mkdir(
                                parents=True, exist_ok=True)
                            pdf = individual_plot_path /\
                                "totalsize_{}_numsegments_{}_recvsize_{}_{}.pdf".format(total_size,
                                                                                        num_segments,
                                                                                        recv_size, metric)

                            total_plot_args = [str(plotting_script),
                                               str(full_log),
                                               str(post_process_log), str(pdf),
                                               metric, "tput_latency",
                                               factor_name, str(
                                total_size),
                                str(num_segments),
                                str(recv_size)]
                            self.run_plot_cmd(total_plot_args)

        elif total_args.looping_variable == "array_total_size":
            for metric in metrics:
                for total_size in TOTAL_SIZES_TO_LOOP:
                    for num_segments in SEGMENTS_TO_LOOP:
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "totalsize_{}".format(total_size) /\
                            "numsegments_{}".format(num_segments)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "totalsize_{}_numsegments_{}_{}.pdf".format(total_size,
                                                                        num_segments, metric)
                        metric_name = metric
                        if metric_name == "tput":
                            metric_name = "tput_pps"
                        total_plot_args = [str(plotting_script), str(full_log),
                                           str(post_process_log), str(pdf),
                                           metric_name, "individual", factor_name, str(
                            total_size),
                            str(num_segments)]
                        self.run_plot_cmd(total_plot_args)

                        # for each array size, plot an individual tput latency
                        for array_size in COMPLETE_ARRAY_SIZES_TO_LOOP:
                            if metric == "tput":
                                continue
                            individual_plot_path = plot_path /\
                                "totalsize_{}".format(total_size) /\
                                "numsegments_{}".format(num_segments) /\
                                "arraysize_{}".format(array_size)
                            individual_plot_path.mkdir(
                                parents=True, exist_ok=True)
                            pdf = individual_plot_path /\
                                "totalsize_{}_numsegments_{}_arraysize_{}_{}.pdf".format(total_size,
                                                                                         num_segments,
                                                                                         array_size, metric)

                            total_plot_args = [str(plotting_script),
                                               str(full_log),
                                               str(post_process_log), str(pdf),
                                               metric, "tput_latency",
                                               factor_name, str(
                                total_size),
                                str(num_segments),
                                str(array_size)]
                            self.run_plot_cmd(total_plot_args)


def main():
    utils.debug('Starting Scatter-Gather bench experiment')
    parser, namespace = runner.get_basic_args()
    scatter_gather = ScatterGather(
        namespace.exp_config,
        namespace.config)
    scatter_gather.execute(parser, namespace)


if __name__ == '__main__':
    main()
