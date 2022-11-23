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
import collections
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
class ScatterGatherIteration(runner.Iteration):
    def __init__(self, 
                 client_rates, 
                 num_threads, 
                 system_name,
                 segment_size,
                 num_segments, 
                 array_size,
                 busy_cycles=0, 
                 recv_pkt_size=0, 
                 num_server_cores=1,
                 trial=None):
        """
        Arguments:
        * client_rates: Mapping from {int, int} specifying rates and how many
        clients send at that rate.
        Total clients cannot exceed the maximum clients on the machine.
        * segment_size: Segment size each client is sending at
        * num_segments: Number of separate scattered buffers the clients are using.
        """
        self.client_rates = client_rates
        self.num_threads = num_threads
        self.segment_size = segment_size
        self.num_segments = num_segments
        self.trial = trial
        self.array_size = array_size
        self.busy_cycles = busy_cycles
        self.recv_pkt_size = recv_pkt_size
        self.num_server_cores=num_server_cores
        self.system = system_name

    def __str__(self):
        return "ScatterGather Iteration info: " \
            "client rates: {} , " \
            "num_threads: {} , " \
            "segment size: {} , " \
            "num server cores: {} , "\
            "system: {} , " \
            "recv pkt size: {}, "\
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_segment_size_string(),
                               self.num_threads,
                               self.get_num_server_cores_string(),
                               self.get_system_string(),
                               self.get_recv_pkt_size_string(),
                               self.get_trial_string())

    def hash(self):
        args = [self.segment_size, 
                self.num_segments, 
                self.array_size,
                self.num_server_cores, 
                self.recv_pkt_size,
                self.busy_cycles,
                self.system,
                self.num_threads, 
                self.get_num_clients(), 
                self.trial]
        return hashlib.md5(json_dumps(args).encode('utf-8')).hexdigest()

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment
        """
        return ["system", 
                "num_segments", 
                "segment_size", 
                "array_size",
                "num_server_cores",
                "busy_cycles",
                "recv_pkt_size",
                "num_threads",
                "num_clients",
                "offered_load_pps",
                "offered_load_gbps"]
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
                "array_size": self.array_size,
                "busy_cycles": self.busy_cycles,
                "recv_pkt_size": self.recv_pkt_size,
                "num_server_cores": self.num_server_cores,
                "system": self.get_system_string(),
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "offered_load_pps": offered_load_pps,
                "offered_load_gbps": offered_load_gbps,
        }
    
    def get_iteration_avg_message_size(self):
        return self.segment_size * self.num_segments

    def get_num_threads(self):
        return self.num_threads
    
    def get_num_server_cores(self):
        return self.num_server_cores
    
    def get_recv_pkt_size(self):
        return self.recv_pkt_size
    
    def get_array_size(self):
        return self.array_size
    
    def get_num_segments(self):
        return self.num_segments
    
    def get_segment_size(self):
        return self.segment_size
    
    def get_busy_cycles(self):
        return self.busy_cycles
    
    def get_num_threads(self):
        return self.num_threads
    
    def get_num_clients(self):
        ret = 0
        for info in self.client_rates:
            ret += info[1]
        return ret
    
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

    def get_num_segments_string(self):
        return "num_segments_{}".format(self.num_segments)
    
    def get_segment_size_string(self):
        return "segment_size_{}".format(self.segment_size)
    
    def get_array_size_string(self):
        return "array_size_{}".format(self.array_size)
    
    def get_num_server_cores_string(self):
        return "server_cores_{}".format(self.num_server_cores)
    
    def get_recv_pkt_size_string(self):
        if self.recv_pkt_size == 0:
            return "recv_size_small"
        else:
            return "recv_size_{}".format(self.recv_pkt_size)

    def get_system_string(self):
        return self.system

    def get_busy_cycles_string(self):
        return "busy_cycles_{}".format(self.busy_cycles)

    def get_num_threads_string(self):
        return "threads_{}".format(self.num_threads)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def get_parent_folder(self, high_level_folder):
        # returned path doesn't include the trial
        path = Path(high_level_folder)
        return path / self.get_system_string() /\
                self.get_array_size_string() /\
                self.get_recv_pkt_size_string() /\
                self.get_busy_cycles_string() /\
                self.get_num_server_cores_string() /\
                self.get_segment_size_string() /\
                self.get_num_segments_string() /\
                self.get_client_rate_string() /\
                self.get_num_threads_string()


    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def get_num_clients(self):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return total_hosts

    def get_program_args(self,
                        host,
                        config_yaml,
                        program,
                        programs_metadata):
        ret = {}
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        # both sides need to know about the server mac address
        ret["server_mac"] = config_yaml["hosts"][server_host]["mac"]
        ret["server_ip"] = config_yaml["hosts"][server_host]["ip"]
        ret = {}
        ret["pci_addr"] = config_yaml["mlx5"]["pci_addr"]
        ret["array_size"] = self.array_size
        ret["num_threads"] = self.num_threads
        ret["num_clients"] = self.get_num_clients()
        ret["num_server_cores"] = self.num_server_cores
        ret["num_segments"] = self.num_segments
        ret["segment_size"] = self.segment_size
        ret["busy_cycles"] = self.busy_cycles
        ret["server_mac"] = config_yaml["hosts"][server_host]["mac"]
        ret["server_ip"] = config_yaml["hosts"][server_host]["ip"]

        if self.system == "echo":
            ret["echo_str"] = " --echo_mode"
        elif self.system == "copy":
            ret["with_copy"] = " --with_copy"
            ret["echo_str"] = ""
        else:
            if self.system != "zero_copy":
                utils.warn("Unknown system name: ", self.system)
                exit(1)
            ret["echo_str"] = ""
            ret["with_copy"] = ""

        if (self.recv_pkt_size != 0):
            ret["read_pkt_str"] = " --read_incoming_packet"
            ret["send_packet_size"] = self.recv_pkt_size
            if self.recv_pkt_size < (4 + 8 * self.num_segments):
                utils.warn("For recv pkt size arg, must be larger than: ", 
                        4 + 8 * self.num_segments)
                exit(1)
        else:
            ret["read_pkt_str"] = ""
            ret["send_packet_size"] = 4 + 8 * self.num_segments

        if program == "start_client":
            host_options = self.get_iteration_clients(
                    host_type_map["client"])
            rate = self.find_rate(host_options, host)
            ret["rate"] = rate
            ret["machine_id"] = self.find_client_id(host_options,host)
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
        return ret
SgBenchInfo = collections.namedtuple("SgBenchInfo",
        ["segment_size",
         "num_segments",
         "array_size",
         "num_server_cores",
         "busy_cycles",
         "recv_pkt_size"])

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
        iteration_hash = iteration.hash()
        try:
            throughput = iteration.read_key_from_analysis_log(higher_level_folder, "achieved_load_gbps")
            percent_achieved = iteration.read_key_from_analysis_log(higher_level_folder, "percent_achieved_rate")
        except:
            utils.warn("Could not read throughput and percent achieved for trial {}".format(str(iteration)))
        if iteration_hash not in self.iteration_skipping_information:
            self.iteration_skipping_information[iteration_hash] = (
                throughput, percent_achieved, False)
            if (throughput == 0 and percent_achieved == 0):
                return
        else:
            # if previous iteration does not meet percent achieved cutoff
            print(self.iteration_skipping_information[iteration_hash])
            if self.iteration_skipping_information[iteration_hash][1] < utils.PERCENT_ACHIEVED_CUTOFF:
                self.iteration_skipping_information[iteration_hash] = (throughput, percent_achieved, False)
            elif self.iteration_skipping_information[iteration_hash][0] > throughput:
                self.iteration_skipping_information[iteration_hash] = (throughput, percent_achieved, True)

    def skip_iteration(self, total_args, iteration):
        if total_args.exp_type == "individual":
            return False
        iteration_hash = iteration.hash()
        if iteration_hash not in self.iteration_skipping_information:
            return False
        else:
            throughput, percent_achieved, skip = self.iteration_skipping_information[
                iteration_hash]
            return skip

    def parse_exp_info_string(self, exp_string):
        """
        Returns parsed SgBenchInfo from exp_string.
        Should be formatted as:
         "segment_size = {}, "\
         "num_segments = {}, "\
         "array_size = {}, "\
         "num_server_cores = {}, "\
         "busy_cycles = {}, "\
         "recv_pkt_size = {}"
        """
        try:
            parse_result = parse.parse("segment_size = {:d}, num_segments = {:d}, array_size = {:d}, num_server_cores = {:d}, busy_cycles = {:d}, recv_pkt_size = {:d}", 
                    exp_string)
            return SgBenchInfo(parse_result[0],
                    parse_result[1],
                    parse_result[2],
                    parse_result[3],
                    parse_result[4],
                    parse_result[5])
        except:
            utils.error("Error parsing exp_string: {}".format(exp_string))
            exit(1)
            

    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > self.config_yaml["max_clients"]:
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)

            client_rates = [(total_args.rate, total_args.num_clients)]
            it = ScatterGatherIteration(
                 client_rates, 
                 total_args.num_threads, 
                 total_args.system,
                 total_args.segment_size,
                 total_args.num_segments, 
                 total_args.array_size,
                 total_args.busy_cycles, 
                 total_args.recv_pkt_size, 
                 total_args.num_server_cores)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            it.set_trial(num_trials_finished)
            return [it]
        else:
            ret = []
            loop_yaml = self.get_loop_yaml()
            num_trials = utils.yaml_get(loop_yaml, "num_trials")
            num_threads = utils.yaml_get(loop_yaml, "num_threads")
            num_clients = utils.yaml_get(loop_yaml, "num_clients")
            rate_percentages = utils.yaml_get(loop_yaml, "rate_percentages")
            systems = utils.yaml_get(loop_yaml, "systems")
            max_rates_dict = self.parse_max_rates(
                     utils.yaml_get(loop_yaml, "max_rates"))

            for trial in range(0, num_trials):
                for system in systems:
                    for sgbenchinfo in max_rates_dict:
                        # iterate in reverse to enable stopping early.
                        for rate_percentage in reversed(rate_percentages):
                            max_rate = max_rates_dict[sgbenchinfo]
                            segment_size = sgbenchinfo.segment_size
                            num_segments = sgbenchinfo.num_segments
                            array_size = sgbenchinfo.array_size
                            busy_cycles = sgbenchinfo.busy_cycles
                            recv_pkt_size = sgbenchinfo.recv_pkt_size
                            num_server_cores = sgbenchinfo.num_server_cores
                            rate = int(float(max_rate) *
                                        rate_percentage)
                            client_rates = [(rate, num_clients)]

                            it = ScatterGatherIteration(
                                    client_rates, 
                                    num_threads,
                                    system,
                                    segment_size,
                                    num_segments,
                                    array_size,
                                    busy_cycles,
                                    recv_pkt_size,
                                    num_server_cores,
                                    trial = trial)
                            ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        if namespace.exp_type == "individual":
            parser.add_argument("-wc", "--with_copy",
                            dest="with_copy",
                            action='store_true',
                            help="Whether the server uses a copy or not.")
            parser.add_argument("-arr", "--array_size",
                            dest="array_size",
                            type=int,
                            default=10000,
                            help="Array size")
            parser.add_argument("-echo", "--echo",
                            dest="echo",
                            action='store_true',
                            help="Whether the server should use zero-copy echo mode or not.")
            parser.add_argument("-rs", "--recv_pkt_size",
                            dest="recv_pkt_size",
                            type=int,
                            default=0,
                            help="If set, receive packet size")
            parser.add_argument("-bc", "--busy_cycles",
                            dest="busy_cycles",
                            type=int,
                            default=0,
                            help="Busy cycles in us")
            parser.add_argument("-nsc", "--num_server_cores",
                            dest="num_server_cores",
                            type=int,
                            default=1,
                            help="Number of server cores.")
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
            parser.add_argument("--system",
                                help = "Which mode to run.",
                                choices = ["copy", "zero_copy", "echo"])
        else:
            parser.add_argument("-l", "--logfile",
                                help="Logfile name",
                                type=utils.check_log_extension,
                                default="summary.log")
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def run_summary_analysis(self, 
            df, 
            out, 
            system,
            num_segments,
            segment_size, 
            array_size, 
            recv_pkt_size, 
            num_server_cores,
            busy_cycles):
        filtered_df = df[(df.array_size == array_size) &
                         (df.recv_pkt_size == recv_pkt_size) &
                         (df.num_segments == num_segments) &
                         (df.segment_size == segment_size) &
                         (df.system == system) &
                         (df.num_server_cores  == num_server_cores) &
                         (df.busy_cycles == busy_cycles)]
        filtered_df = filtered_df[filtered_df["percent_achieved_rate"] >= .95]
        # calculate lowest rate, get p99 and median
        # stats
        min_rate = filtered_df["offered_load_pps"].min()
        latency_df = filtered_df[(filtered_df.offered_load_pps == min_rate)]
        p99_mean = latency_df["p99"].mean()
        p99_sd = latency_df["p99"].std(ddof=0)
        median_mean = latency_df["median"].mean()
        median_sd = latency_df["median"].std(ddof=0)
        def ourstd(x):
            return np.std(x, ddof=0)

        clustered_df = filtered_df.groupby([
            "num_segments",
            "segment_size",
            "array_size",
            "recv_pkt_size",
            "busy_cycles",
            "num_server_cores",
            "system",
            "offered_load_pps",
            "offered_load_gbps",
            ], as_index = False).agg(
            achieved_load_pps_mean = \
                pd.NamedAgg(column="achieved_load_pps",
                aggfunc="mean"),
            achieved_load_pps_sd = \
                pd.NamedAgg(column="achieved_load_pps",
                aggfunc=ourstd),
            percent_achieved_rate= \
                pd.NamedAgg(column="percent_achieved_rate",
                aggfunc='mean'),
            achieved_load_gbps_mean= \
                pd.NamedAgg(column="achieved_load_gbps",
                aggfunc="mean"),
            achieved_load_gbps_sd = \
                pd.NamedAgg(column="achieved_load_gbps",
                aggfunc=ourstd))
        max_achieved_pps = clustered_df["achieved_load_pps_mean"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_mean"].max()
        std_achieved_pps = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'achieved_load_pps_sd']
        std_achieved_gbps = clustered_df.loc[clustered_df['achieved_load_gbps_mean'].idxmax(),
                                             'achieved_load_gbps_sd']
        
        out.write(str(system) + "," + \
                    str(segment_size) + "," + \
                    str(num_segments) + "," + \
                    str(array_size) + "," + \
                    str(recv_pkt_size) + "," + \
                    str(busy_cycles) + "," + \
                    str(num_server_cores) + "," + \
                    str(p99_mean) + "," + \
                    str(p99_sd) + "," + \
                    str(median_mean) + "," + \
                    str(median_sd) + "," + \
                    str(max_achieved_pps) + "," + \
                    str(max_achieved_gbps) + "," + \
                    str(std_achieved_pps) + "," + \
                    str(std_achieved_gbps) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        utils.info("Running post process analysis")
        header_str = "segment_size,num_segments,array_size,recv_pkt_size,busy_cycles,num_server_cores,mp99,p99sd,mmedian,mediansd,maxtputpps,maxtputgbps,maxtputppssd,maxtputgbpssd" + os.linesep
        
        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)
        loop_yaml = self.get_loop_yaml()
        systems = utils.yaml_get(loop_yaml, "systems")
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
        for system in systems:
            for sgbenchinfo in max_rates_dict:
                self.run_summary_analysis(df,
                                        out,
                                        system,
                                        sgbenchinfo.num_segments,
                                        sgbenchinfo.segment_size,
                                        sgbenchinfo.array_size,
                                        sgbenchinfo.recv_pkt_size,
                                        sgbenchinfo.num_server_cores,
                                        sgbenchinfo.busy_cycles)
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
        # TODO: correct this at some point
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
