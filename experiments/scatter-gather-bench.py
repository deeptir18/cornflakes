from main import runner
from main import utils
import heapq
import yaml
from pathlib import Path
import os
import parse
STRIP_THRESHOLD = 0.03


def parse_client_time_and_pkts(line):
    fmt = parse.compile("Ran for {} seconds, sent {} packets.")
    time, pkts_sent = fmt.parse(line)
    return (time, pkts_sent)


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
        return ret


class ScatterGatherIteration(runner.Iteration):
    def __init__(self, client_rates, segment_size,
                 num_mbufs, with_copy, trial=None):
        """
        Arguments:
        * client_rates: Mapping from {int, int} specifying rates and how many
        clients send at that rate.
        Total clients cannot exceed the maximum clients on the machine.
        * segment_size: Segment size each client is sending at
        * num_mbufs: Number of separate scattered buffers the clients are using.
        * with_copy: Whether the server is copying out the payload.
        """
        self.client_rates = client_rates
        self.segment_size = segment_size
        self.num_mbufs = num_mbufs
        self.with_copy = with_copy
        self.trial = trial

    def get_segment_size(self):
        return self.segment_size

    def get_num_mbufs(self):
        return self.num_mbufs

    def get_with_copy(self):
        return self.with_copy

    def set_trial(self, trial):
        self.trial = trial

    def get_total_size(self):
        return self.num_mbufs * self.segment_size

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
            return "with_copy"
        else:
            return "zero_copy"

    def get_segment_size_string(self):
        return "segmentsize_{}".format(self.segment_size)

    def get_num_mbufs_string(self):
        # if with_copy is turned on, server copies in segment size increments
        return "mbufs_{}".format(self.num_mbufs)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def __str__(self):
        return "Iteration info: client rates: {}, " \
            "segment size: {}, " \
            " num_mbufs: {}, " \
            " with_copy: {}" \
            " trial: {}".format(self.get_client_rate_string(),
                                self.get_segment_size_string(),
                                self.get_num_mbufs_string(),
                                self.get_with_copy_string(),
                                self.get_trial_string())

    def get_parent_folder(self, high_level_folder):
        # returned path doesn't include the trial
        path = Path(high_level_folder)
        return path / self.get_segment_size_string() / self.get_num_mbufs_string() / self.get_client_rate_string() / self.get_with_copy_string()

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

    def get_program_args(self,
                         folder,
                         program,
                         host,
                         config_yaml,
                         programs_metadata,
                         exp_time):
        ret = {}
        if program == "start_server":
            ret["cornflakes_dir"] = config_yaml["cornflakes_dir"]
            ret["server_ip"] = config_yaml["hosts"][host]["ip"]
            if self.with_copy:
                ret["with_copy"] == " --with_copy"
            else:
                ret["with_copy"] = ""
            ret["folder"] = str(folder)
        elif program == "start_client":
            utils.debug("Host name: ", host)
            # calculate client rate
            host_options = self.get_iteration_clients(
                programs_metadata[program]["hosts"])
            rate = self.find_rate(host_options, host)
            idx = 0
            server_host = programs_metadata["start_server"]["hosts"][0]
            ret["cornflakes_dir"] = config_yaml["cornflakes_dir"]
            ret["server_ip"] = config_yaml["hosts"][server_host]["ip"]
            ret["host_ip"] = config_yaml["hosts"][host]["ip"]
            ret["server_mac"] = config_yaml["hosts"][server_host]["mac"]
            ret["rate"] = rate
            ret["segment_size"] = self.segment_size
            ret["num_mbufs"] = self.num_mbufs
            ret["time"] = exp_time
            ret["latency_log"] = "{}.latency.log".format(host)
            ret["host"] = host
            ret["folder"] = str(folder)
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
                                        total_args.num_mbufs,
                                        total_args.with_copy)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            it.set_trial(num_trials_finished)
            return [it]
        else:
            # todo: loop over parameters
            it = ScatterGatherIteration([(300000, 1)],
                                        1024, 4, False)
            num_trials_finished = utils.parse_number_trials_done(
                it.get_parent_folder(total_args.folder))
            it.set_trial(num_trials_finished)
            it2 = ScatterGatherIteration([(300000, 1)],
                                         1024, 4, False)
            it2.set_trial(num_trials_finished + 1)
            return [it, it2]

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-wc", "--with_copy",
                            dest="with_copy",
                            action='store_true',
                            help="Whether the server uses a copy or not.")
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
            parser.add_argument("-m", "--num_mbufs",
                                help="Number of mbufs",
                                type=int,
                                default=1)
            parser.add_argument('-nc', "--num_clients",
                                help="Number of clients",
                                type=int,
                                default=1)
        else:
            parser.add_argument("-l", "--logfile",
                                help="Logfile name",
                                default="summary.log")
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "segment_size,num_mbufs,with_copy," \
            "offered_load_pps,offered_load_gbps," \
            "achieved_load_pps,achieved_load_gbps," \
            "percent_acheived_rate," \
            "avg,p99,median"

    def run_analysis_individual_trial(self,
                                      higher_level_folder,
                                      program_metadata,
                                      iteration,
                                      print_stats=False):
        exp_folder = iteration.get_folder_name(higher_level_folder)
        # parse each client log
        total_achieved_load = 0
        total_offered_rate = 0
        total_offered_load = 0
        total_achieved_rate = 0
        client_latency_lists = []
        clients = iteration.get_iteration_clients(
            program_metadata["start_client"]["hosts"])

        for host in clients:
            args = {"folder": str(exp_folder), "host": host}

            stdout_log = program_metadata["start_client"]["log"]["out"].format(
                **args)
            stdout_info = parse_log_info(stdout_log)
            if stdout_info == {}:
                utils.warn("Error parsing stdout log {}".format(stdout_log))
                return ""

            run_metadata_log = program_metadata["start_client"]["log"]["record"].format(
                **args)
            run_info = utils.parse_command_line_args(run_metadata_log)
            if run_info == {}:
                utils.warn("Error parsing yaml run info for {}".format(
                    run_metadata_log))
                return ""

            latency_log = "{folder}/{host}.latency.log".format(**args)
            latencies = utils.parse_latency_log(latency_log, STRIP_THRESHOLD)
            if latencies == []:
                utils.warn("Error parsing latency log {}".format(latency_log))
                return ""
            client_latency_lists.append(latencies)

            host_offered_rate = float(run_info["args"]["rate"])
            total_offered_rate += host_offered_rate
            host_offered_load = float(utils.get_tput_gbps(host_offered_rate,
                                      iteration.get_total_size()))
            total_offered_load += host_offered_load

            host_pkts_sent = stdout_info["pkts_sent"]
            host_total_time = stdout_info["totaltime"]
            host_achieved_rate = float(host_pkts_sent) / float(host_total_time)
            total_achieved_rate += host_achieved_rate
            host_achieved_load = utils.get_tput_gbps(
                host_achieved_rate,
                iteration.get_total_size())
            total_achieved_load += host_achieved_load

            host_p99 = utils.p99_func(latencies) / 1000.0
            host_median = utils.median_func(latencies) / 1000.0
            host_avg = utils.mean_func(latencies) / 1000.0
            host_pkts_sent = stdout_info["pkts_sent"] / 1000.0
            host_total_time = stdout_info["totaltime"] / 1000.0

            if print_stats:
                utils.info("Client {}: "
                           "offered load: {:.2f} req/s | {:.2f} Gbps, "
                           "achieved load: {:.2f} req/s | {:.2f} Gbps, "
                           "percentage achieved rate: {:.3f},"
                           "avg latency: {:.2f} us, p99: {:.2f} us, median: {:.2f} us".format(
                               host, host_offered_rate, host_offered_load,
                               host_achieved_rate, host_achieved_load,
                               float(host_achieved_rate / host_offered_rate),
                               host_avg, host_p99, host_median))
        # print total stats
        sorted_latencies = list(heapq.merge(*client_latency_lists))
        median = utils.median_func(sorted_latencies) / float(1000)
        p99 = utils.p99_func(sorted_latencies) / float(1000)
        avg = utils.mean_func(sorted_latencies) / float(1000)

        if print_stats:
            total_stats = "offered load: {:.2f} req/s | {:.2f} Gbps, "  \
                "achieved load: {:.2f} req/s | {:.2f} Gbps, " \
                "percentage achieved rate: {:.3f}," \
                "avg latency: {:.2f} us, p99: {:.2f} us, median: {:.2f} us".format(
                    total_offered_rate, total_offered_load,
                    total_achieved_rate, total_achieved_load,
                    float(total_achieved_rate / total_offered_rate),
                    avg, p99, median)
            utils.info("Total Stats: ", total_stats)
        csv_line = "{},{},{},{},{},{},{},{},{},{}".format(iteration.get_segment_size(),
                                                          iteration.get_num_mbufs(),
                                                          iteration.get_with_copy(),
                                                          total_offered_rate,
                                                          total_offered_load,
                                                          total_achieved_rate,
                                                          total_achieved_load,
                                                          avg * 1000,
                                                          p99 * 1000,
                                                          median * 1000)
        return csv_line


def main():
    parser, namespace = runner.get_basic_args()
    scatter_gather = ScatterGather(
        namespace.exp_config,
        namespace.config)
    scatter_gather.execute(parser, namespace)


if __name__ == '__main__':
    main()
