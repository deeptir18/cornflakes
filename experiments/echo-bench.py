from main import runner, utils
import heapq
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
import copy
import pandas as pd
import numpy as np
STRIP_THRESHOLD = 0.03

# SIZES_TO_LOOP = [1024, 2048, 4096, 8192]
NUM_THREADS = 4
NUM_CLIENTS = 2
NUM_CLIENTS_MOTIVATION = 3
SIZES_TO_LOOP = [512, 4096]
MESSAGE_TYPES = ["single"]
MESSAGE_TYPES.extend(["list-2", "list-4", "list-8",
                     "tree-2", "tree-1", "tree-3"])
rates = [2500, 5000, 10000, 15000, 25000, 35000, 45000, 55000,
         65000, 75000, 85000, 95000, 105000, 115000, 125000,
         135000, 145000, 155000, 165000,
         175000, 185000, 200000, 220000, 240000, 280000, 300000, 320000, 360000, 400000]
SERIALIZATION_LIBRARIES = ["cornflakes-dynamic",  # "cereal", "capnproto", "protobuf",
                           # "flatbuffers",  # "cornflakes-fixed",
                           # "cornflakes1c-fixed"]  # "cornflakes1c-fixed", "protobuf", "capnproto",
                           "cornflakes1c-dynamic"]
MOTIVATION_SERIALIZATION_LIBRARIES = ["ideal", "onecopy", "twocopy",
                                      "flatbuffers", "capnproto", "cereal", "protobuf"]
ALL_SERIALIZATION_LIBRARIES = [
    "cornflakes-dynamic", "cornflakes1c-dynamic", "flatbuffers", "capnproto",
    "cereal", "protobuf", "onecopy", "twocopy", "ideal"]
MOTIVATION_MESSAGE_TYPE = "list-2"
MOTIVATION_SIZES_TO_LOOP = [1024, 4096]


def parse_client_time_and_pkts(line):
    line_split = line.split("High level sending stats ")[1]
    fmt = parse.compile("sent={} received={}"
                        " retries={} unique_sent={} total_time={}")
    sent, recved, retries, unique_set, total_time = fmt.parse(
        line_split)
    return (sent, recved, retries, total_time)


def parse_log_info(log):
    if not(os.path.exists(log)):
        utils.warn("Path {} does not exist.".format(log))
        return {}
    ret = {}
    with open(log, 'r') as f:
        raw_lines = f.readlines()
        lines = [line.strip() for line in raw_lines]
        for line in lines:
            if "High level sending stats" in line:
                (sent, recved, retries, total_time) = parse_client_time_and_pkts(line)
                ret["sent"] = sent
                ret["recved"] = recved
                ret["retries"] = retries
                ret["total_time"] = total_time
        return ret


class EchoBenchIteration(runner.Iteration):
    def __init__(self, client_rates, size,
                 serialization, message_type,
                 num_threads,
                 trial=None,
                 ref_counting=True):
        """
        Arguments:
        * client_rates: Mapping from {int, int} specifying rates and how many
        clients send at that rate. Total clients cannot exceed macximum clients
        on the machine.
        * size: Total size of the data structure to be echoed (not including
        additional header space the serialization library will use).
        * serialization: Serialization library to use.
        * message_type: Type of data structure to echo.
        mode is used. For cornflakes, one of first two must be enabled.
        zero-copy send from zero-copy receive.
        """
        self.client_rates = client_rates
        self.size = size
        self.serialization = serialization
        self.message_type = message_type
        self.trial = trial
        self.num_threads = num_threads
        self.ref_counting = ref_counting

    def get_num_threads(self):
        return self.num_threads

    def get_size(self):
        return self.size

    def get_serialization(self):
        return self.serialization

    def get_message_type(self):
        return self.message_type

    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_ref_counting(self):
        return self.ref_counting

    def get_num_threads_string(self):
        return "{}_threads".format(self.num_threads)

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

    def get_size_string(self):
        return "size_{}".format(self.size)

    def ref_counting_string(self):
        return "ref_counting_{}".format(self.ref_counting)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def __str__(self):
        return "Iteration info: client rates: {}, "\
            "size: {}, " \
            "serialization: {}, " \
            "message_type: {}, " \
            "num_threads: {}, " \
            "ref counting: {}," \
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_size_string(),
                               self.serialization,
                               self.message_type,
                               self.num_threads,
                               self.ref_counting,
                               self.get_trial_string())

    def get_serialization_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization / self.message_type /\
            self.get_size_string() / self.get_client_rate_string() /\
            self.get_num_threads_string() / self.ref_counting_string()

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

    def find_client_id(self, client_options, host):
        # TODO: what if this doesn't work
        return client_options.index(host)

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
        ret["cornflakes_dir"] = config_yaml["cornflakes_dir"]
        ret["config_file"] = config_yaml["config_file"]
        ret["size"] = "{}".format(self.size)
        ret["library"] = self.serialization
        ret["client-library"] = self.serialization
        ret["message"] = self.message_type
        ret["folder"] = str(folder)

        if ret["library"] == "cornflakes-dynamic":
            ret["client-library"] = "cornflakes1c-dynamic"
        elif ret["library"] == "cornflakes-fixed":
            ret["client-library"] = "cornflakes1c-fixed"

        if program == "start_server":
            if not(self.ref_counting):
                ret["no_ref_counting"] = " --no_ref_counting"
            else:
                ret["no_ref_counting"] = ""
        elif program == "start_client":
            # calculate client rate
            host_options = self.get_iteration_clients(
                programs_metadata[program]["hosts"])
            rate = self.find_rate(host_options, host)
            ret["rate"] = rate
            ret["num_threads"] = self.num_threads
            ret["num_machines"] = self.get_num_clients()
            ret["machine_id"] = self.find_client_id(host_options, host)

            # calculate server host
            server_host = programs_metadata["start_server"]["hosts"][0]
            ret["server_ip"] = config_yaml["hosts"][server_host]["ip"]

            # exp time
            ret["time"] = exp_time
            ret["host"] = host
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret


class EchoBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "DSEchoBench"
        self.config_yaml = yaml.load(Path(config_yaml).read_text(),
                Loader=yaml.FullLoader)
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text(),
                Loader=yaml.FullLoader)

    def experiment_name(self):
        return self.exp

    def get_git_directories(self):
        directory = self.config_yaml["cornflakes_dir"]
        return [directory]

    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > int(self.config_yaml["max_clients"]):
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)
            client_rates = [(total_args.rate, total_args.num_clients)]
            it = EchoBenchIteration(client_rates,
                                    total_args.size,
                                    total_args.serialization,
                                    total_args.message_type,
                                    total_args.num_threads,
                                    ref_counting=not(total_args.no_ref_counting))
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            if total_args.analysis_only or total_args.graph_only:
                ret = []
                for i in range(0, num_trials_finished):
                    it_clone = copy.deepcopy(it)
                    it_clone.set_trial(i)
                    ret.append(it_clone)
                return ret
            it.set_trial(num_trials_finished)
            return [it]

        else:
            # loop over the options
            ret = []
            if total_args.loop_mode == "eval":
                for trial in range(utils.NUM_TRIALS):
                    for serialization in SERIALIZATION_LIBRARIES:
                        for rate in rates:
                            client_rate = [(rate, NUM_CLIENTS)]
                            num_threads = NUM_THREADS
                            for size in SIZES_TO_LOOP:
                                for message_type in MESSAGE_TYPES:
                                    if size == 8192\
                                            and message_type == "tree-5"\
                                            and (serialization == "cornflakes-dynamic" or serialization == "cornflakes-1cdynamic"):
                                        continue
                                    it = EchoBenchIteration(client_rate,
                                                            size,
                                                            serialization,
                                                            message_type,
                                                            num_threads,
                                                            trial=trial,
                                                            ref_counting=not(total_args.no_ref_counting))
                                    ret.append(it)
            elif total_args.loop_mode == "motivation":
                for trial in range(utils.NUM_TRIALS):
                    for serialization in MOTIVATION_SERIALIZATION_LIBRARIES:
                        for rate in rates:
                            client_rate = [(rate, NUM_CLIENTS_MOTIVATION)]
                            num_threads = NUM_THREADS
                            message_type = MOTIVATION_MESSAGE_TYPE
                            for size in MOTIVATION_SIZES_TO_LOOP:
                                it = EchoBenchIteration(client_rate,
                                                        size,
                                                        serialization,
                                                        message_type,
                                                        num_threads,
                                                        trial=trial,
                                                        ref_counting=not(total_args.no_ref_counting))
                                ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        parser.add_argument("-nrc", "--no_ref_counting",
                            dest="no_ref_counting",
                            action='store_true',
                            help="Turn off reference counting in server.")
        if namespace.exp_type == "individual":
            parser.add_argument("-nt", "--num_threads",
                                dest="num_threads",
                                type=int,
                                default=1,
                                help="Number of threads to run with")
            parser.add_argument("-r", "--rate",
                                dest="rate",
                                type=int,
                                default=60000,
                                help="Rate of client(s) in (pkts/sec).")
            parser.add_argument("-s", "--size",
                                dest="size",
                                help="Total message size.",
                                required=True)
            parser.add_argument("-m", "--message_type",
                                dest="message_type",
                                choices=MESSAGE_TYPES,
                                required=True)
            parser.add_argument("-nc", "--num_clients",
                                dest="num_clients",
                                type=int,
                                default=1)
            parser.add_argument("-ser", "--serialization",
                                dest="serialization",
                                choices=ALL_SERIALIZATION_LIBRARIES,
                                required=True)
        else:
            parser.add_argument("-lm", "--loop_mode",
                                dest="loop_mode",
                                choices=["eval", "motivation"],
                                default="eval",
                                help="looping mode variable")
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "serialization,refcounting,message_type,size,"\
            "offered_load_pps,offered_load_gbps,"\
            "achieved_load_pps,achieved_load_gbps,"\
            "percent_achieved_rate,total_retries,"\
            "avg,median,p99,p999"

    def run_summary_analysis(self, df, out, size, message_type, serialization):
        filtered_df = df[(df["serialization"] == serialization) &
                         (df["size"] == size) &
                         (df["message_type"] == message_type)]
        print(size, message_type, serialization)
        # calculate lowest rate, get p99 and median
        # filtered_df = filtered_df[filtered_df["percent_achieved_rate"] >= .95]

        def ourstd(x):
            return np.std(x, ddof=0)

        # CURRENT KNEE CALCULATION:
        # just find maximum achieved rate across all rates
        # group by array size, num segments, segment size,  # average
        clustered_df = filtered_df.groupby(["serialization",
                                            "size", "message_type",
                                           "offered_load_pps",
                                            "offered_load_gbps"],
                                           as_index=False).agg(
            achieved_load_pps_mean=pd.NamedAgg(column="achieved_load_pps",
                                               aggfunc="mean"),
            achieved_load_pps_sd=pd.NamedAgg(column="achieved_load_pps",
                                             aggfunc=ourstd),
            achieved_load_gbps_mean=pd.NamedAgg(column="achieved_load_gbps",
                                                aggfunc="mean"),
            percent_achieved_rate=pd.NamedAgg(column="percent_achieved_rate",
                                              aggfunc="mean"),
            achieved_load_gbps_sd=pd.NamedAgg(column="achieved_load_gbps",
                                              aggfunc=ourstd))

        clustered_df = clustered_df[clustered_df["percent_achieved_rate"] >= 0.95]
        max_achieved_pps = clustered_df["achieved_load_pps_mean"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_mean"].max()
        std_achieved_pps = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'achieved_load_pps_sd']
        std_achieved_gbps = clustered_df.loc[clustered_df['achieved_load_gbps_mean'].idxmax(),
                                             'achieved_load_gbps_sd']
        as_one = False
        out.write(str(serialization) + "," + str(message_type) + "," +
                  str(size) + "," +
                  str(max_achieved_pps) + "," +
                  str(max_achieved_gbps) + "," +
                  str(std_achieved_pps) + "," +
                  str(std_achieved_gbps) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        if total_args.loop_mode == "motivation":
            return
        # need to determine: just knee of curve for each situation
        header_str = "serialization,message_type,size,"\
            "maxtputpps,maxtputgbps,maxtputppssd,maxtputgbpssd" + os.linesep
        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)

        for size in SIZES_TO_LOOP:
            for message_type in MESSAGE_TYPES:
                for serialization in ALL_SERIALIZATION_LIBRARIES:
                    self.run_summary_analysis(
                        df, out, size, message_type, serialization)
        out.close()

    def run_analysis_individual_trial(self,
                                      higher_level_folder,
                                      program_metadata,
                                      iteration,
                                      print_stats=False):
        exp_folder = iteration.get_folder_name(higher_level_folder)

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

        for host in clients:
            args = {"folder": str(exp_folder), "host": host}
            thread_file = "{folder}/{host}.threads.log".format(**args)
            for thread in range(iteration.get_num_threads()):
                args["thread"] = thread  # replace thread number
                latency_log = "{folder}/{host}.latency-t{thread}.log".format(
                    **args)
                latencies = utils.parse_latency_log(
                    latency_log, STRIP_THRESHOLD)
                if latencies == []:
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

                # convert to microseconds
                host_p99 = utils.p99_func(latencies) / 1000.0
                host_p999 = utils.p999_func(latencies) / 1000.0
                host_median = utils.median_func(latencies) / 1000.0
                host_avg = utils.mean_func(latencies) / 1000.0

                # add retries
                retries = int(thread_info["retries"])
                total_retries += retries

                if print_stats:
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
            total_stats = "offered load: {:.4f} req/s | {:.4f} Gbps, "  \
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
        csv_line = "{},{},{},{},{},{},{},{},{},{},{},{},{},{}".format(iteration.get_serialization(),
                                                                      iteration.get_ref_counting(),
                                                                      iteration.get_message_type(),
                                                                      iteration.get_size(),
                                                                      total_offered_load_pps,
                                                                      total_offered_load_gbps,
                                                                      total_achieved_load_pps,
                                                                      total_achieved_load_gbps,
                                                                      percent_acheived_load,
                                                                      total_retries,
                                                                      avg,
                                                                      median,
                                                                      p99,
                                                                      p999)
        return csv_line

    def graph_results(self, args, folder, logfile, post_process_logfile):
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        post_process_log = Path(folder) / post_process_logfile
        plotting_script = Path(cornflakes_repo) / \
            "experiments" / "plotting_scripts" / "echo_bench.R"
        base_args = [str(plotting_script), str(full_log)]
        metrics = ["p99", "median"]

        # make total plot
        for metric in metrics:
            utils.debug("Summary plot for ", metric)
            total_pdf = plot_path / "summary_{}.pdf".format(metric)
            total_plot_args = [str(plotting_script),
                               str(full_log),
                               str(post_process_log),
                               str(total_pdf),
                               metric, "full"]
            print(" ".join(total_plot_args))
            sh.run(total_plot_args)
        # make individual plots
        if args.loop_mode == "motivation":
            for metric in metrics:
                for size in MOTIVATION_SIZES_TO_LOOP:
                    message_type = MOTIVATION_MESSAGE_TYPE
                    individual_plot_path = plot_path / \
                        "size_{}".format(size) / \
                        "msg_{}".format(message_type)
                    individual_plot_path.mkdir(parents=True, exist_ok=True)
                    pdf = individual_plot_path /\
                        "size_{}_msg_{}_{}.pdf".format(
                            size, message_type, metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(post_process_log),
                                       str(pdf),
                                       metric, "motivation", message_type,
                                       str(size)]
                    utils.info("Running: {}".format(" ".join(total_plot_args)))

                    sh.run(total_plot_args)

        elif args.loop_mode == "eval":
            # make summary plots
            for size in SIZES_TO_LOOP:
                for summary_type in ["tree-compare", "list-compare"]:
                    individual_plot_path = plot_path / \
                        "size_{}".format(size)
                    individual_plot_path.mkdir(parents=True, exist_ok=True)
                    pdf = individual_plot_path /\
                        "size_{}_{}_tput.pdf".format(size, summary_type)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(post_process_log),
                                       str(pdf),
                                       "foo", summary_type, str(size)
                                       ]
                    utils.info("Running: {}".format(" ".join(total_plot_args)))
                    sh.run(total_plot_args)
            # just return here for now
            return
            for metric in metrics:
                for size in SIZES_TO_LOOP:
                    for message_type in MESSAGE_TYPES:
                        individual_plot_path = plot_path / \
                            "size_{}".format(size) / \
                            "msg_{}".format(message_type)
                        individual_plot_path.mkdir(parents=True, exist_ok=True)
                        pdf = individual_plot_path /\
                            "size_{}_msg_{}_{}.pdf".format(
                                size, message_type, metric)
                        total_plot_args = [str(plotting_script),
                                           str(full_log),
                                           str(post_process_log),
                                           str(pdf),
                                           metric, "individual", message_type,
                                           str(size)]

                        print(" ".join(total_plot_args))
                        sh.run(total_plot_args)


def main():
    parser, namespace = runner.get_basic_args()
    echo_bench = EchoBench(
        namespace.exp_config,
        namespace.config)
    echo_bench.execute(parser, namespace)


if __name__ == '__main__':
    main()
