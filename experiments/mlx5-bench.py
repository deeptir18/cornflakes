from main import runner
import time
from main import utils
import heapq
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
STRIP_THRESHOLD = 0.03

SEGMENT_SIZES_TO_LOOP = [64, 128, 256, 512, 1024, 2048, 4096, 8192]
TOTAL_SIZES_TO_LOOP = [256, 512, 1024, 2048, 4096, 8192]
# NB_SEGMENTS_TO_LOOP = [1, 2, 4, 8, 16]
NB_SEGMENTS_TO_LOOP = [1, 2, 4, 8]
MAX_CLIENT_RATE_PPS = 200000
MAX_RATE_GBPS = 5  # TODO: should be configured per machine
MIN_RATE_PPS = 5000
MAX_RATE_PPS = 200000
MAX_PKT_SIZE = 8192
MBUFS_MAX = 5
NUM_THREADS = 4
NUM_CLIENTS = 3
# L1 cache = 32K, L2 = 1024K, L3 = ~14080K
ARRAY_SIZES_TO_LOOP = [65536, 819200, 4096000, 65536000, 655360000]
rates = [5000, 10000, 50000, 100000, 200000,
         300000, 400000, 410000, 420000, 431000]
# max rates to get "knee" (for smallest working set size, 0 extra busy work)
max_rates = {256: 425000, 512: 400000, 1024: 375000, 2048: 350000, 4096: 225000,
             8192: 150000}
sample_percentages = [10, 30, 50, 65, 70, 75, 85, 90, 95, 100]


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
                 num_segments, with_copy, as_one, num_threads, trial=None, array_size=8192):
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

    def get_num_threads(self):
        return self.num_threads

    def get_num_clients(self):
        ret = 0
        for info in self.client_rates:
            ret += info[1]
        return ret

    def get_array_size(self):
        return self.array_size

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

    def get_relevant_hosts(self, programs_metadata, program):
        if program == "start_server":
            return programs_metadata["hosts"]
        elif program == "start_client":
            return self.get_iteration_clients(programs_metadata["hosts"])
        else:
            utils.debug("Passed in unknown program name: {}".format(program))

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
        return "Iteration info: client rates: {}, " \
            "segment size: {}, " \
            " num_segments: {}, " \
            " with_copy: {}," \
            "array size: {}," \
            "num client threads: {}," \
            " trial: {}".format(self.get_client_rate_string(),
                                self.get_segment_size_string(),
                                self.get_num_segments_string(),
                                self.get_with_copy_string(),
                                self.get_array_size(),
                                self.get_num_threads(),
                                self.get_trial_string())

    def get_size_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.get_segment_size_string()

    def get_parent_folder(self, high_level_folder):
        # returned path doesn't include the trial
        path = Path(high_level_folder)
        return path / self.get_segment_size_string() /\
            self.get_num_segments_string() / self.get_array_size_string() /\
            self.get_client_rate_string() / self.get_num_threads_string() / \
            self.get_with_copy_string()

    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def get_hosts(self, program, programs_metadata):
        ret = []
        if program == "start_server":
            return [programs_metadata[program]["hosts"][0]]
        elif program == "start_client":
            options = programs_metadata[program]["hosts"]
            return self.get_iteration_clients(options)
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret

    def find_client_id(self, host):
        return int(host[len(host) - 1])

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
        ret["num_machines"] = NUM_CLIENTS
        ret["random_seed"] = time.time()
        # both sides need to know about the server mac address
        server_host = programs_metadata["start_server"]["hosts"][0]
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
        ret["folder"] = str(folder)
        if program == "start_server":
            pass
        elif program == "start_client":
            # calculate client rate
            host_options = self.get_iteration_clients(
                programs_metadata[program]["hosts"])
            rate = self.find_rate(host_options, host)
            ret["host_id"] = self.find_client_id(host)
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
        self.config_yaml = yaml.load(Path(config_yaml).read_text())
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text())

    def experiment_name(self):
        return self.exp

    def get_git_directories(self):
        directory = self.config_yaml["cornflakes_dir"]
        return [directory]

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
                                        array_size=total_args.array_size)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            it.set_trial(num_trials_finished)
            return [it]
        else:
            ret = []
            # for each array size, splits total size into various segments
            if total_args.looping_variable == "array_total_size":
                for trial in range(utils.NUM_TRIALS):
                    for array_size in ARRAY_SIZES_TO_LOOP:
                        for total_size in TOTAL_SIZES_TO_LOOP:
                            max_rate = max_rates[total_size]
                            # sample 10 rates between minimum and max rate,
                            # but sample more towards the max rate
                            for sampling in sample_percentages:
                                rate = int(float(sampling/100) *
                                           max_rate)
                                for with_copy in [False, True]:
                                    for num_segments in NB_SEGMENTS_TO_LOOP:
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
                                default="summary.log")
            parser.add_argument("-lp", "--looping_variable",
                                dest="looping_variable",
                                choices=["array_total_size"],
                                default="segment_size",
                                help="What variable to loop over")
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "segment_size,num_segments,with_copy,as_one,array_size," \
            "num_threads,num_clients,offered_load_pps,offered_load_gbps," \
            "achieved_load_pps,achieved_load_gbps," \
            "percent_acheived_rate," \
            "avg,median,p99,p999"

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
            total_stats = "offered load: {:.4f} req/s | {:.4f} Gbps, " \
                "achieved load: {:.4f} req/s | {:.4f} Gbps, " \
                "percentage achieved rate: {:.4f}," \
                "retries: {}, " \
                "avg latency: {:.4f} \u03BCs, p99: {:.4f} \u03BCs, p999: {:.4f}" \
                "\u03BCs, median: {:.4f} \u03BCs".format(
                    total_offered_load_pps, total_offered_load_gbps,
                    total_achieved_load_pps, total_achieved_load_gbps,
                    float(total_achieved_load_pps / total_offered_load_pps),
                    total_retries,
                    avg, p99, p999, median)
            utils.info("Total Stats: ", total_stats)
        percent_acheived_load = float(total_achieved_load_pps /
                                      total_offered_load_pps)

        csv_line = "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(iteration.get_segment_size(),
                                                                            iteration.get_num_segments(),
                                                                            iteration.get_with_copy(),
                                                                            iteration.get_as_one(),
                                                                            iteration.get_array_size(),
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

    def graph_results(self, total_args, folder, logfile):
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile

        if total_args.looping_variable == "array_total_size":
            # implement graphing for this case: how do we visualize array size
            # and cross of segments at the same time?
            plotting_script = Path(cornflakes_repo) / \
                "experiments" / "plotting_scripts" / "sg_bench.R"
            metrics = ["median", "p99"]

            for metric in metrics:
                output_file = plot_path / "summary_{}.pdf".format(metric)
                args = [str(plotting_script), str(full_log), str(output_file),
                        metric, "full"]
                try:
                    print(" ".join(args))
                    sh.run(args)
                except:
                    utils.warn(
                        "Failed to run plot command: {}".format(args))

            for metric in metrics:
                for total_size in TOTAL_SIZES_TO_LOOP:
                    for num_segments in NB_SEGMENTS_TO_LOOP:
                        segment_size = int(total_size / num_segments)
                        individual_plot_path = plot_path /\
                            "totalsize_{}".format(total_size) /\
                            "numsegments_{}".format(num_segments)
                        individual_plot_path.mkdir(
                            parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "totalsize_{}_numsegments_{}_{}.pdf".format(total_size,
                                                                        num_segments, metric)
                        total_plot_args = [str(plotting_script),
                                           str(full_log), str(pdf),
                                           metric, "individual", str(
                            total_size),
                            str(num_segments)]
                        try:
                            print(" ".join(total_plot_args))
                            sh.run(total_plot_args)
                        except:
                            utils.warn(
                                "Failed to run plot command: {}".format(args))


def main():
    utils.debug('Starting Scatter-Gather bench experiment')
    parser, namespace = runner.get_basic_args()
    scatter_gather = ScatterGather(
        namespace.exp_config,
        namespace.config)
    scatter_gather.execute(parser, namespace)


if __name__ == '__main__':
    main()
