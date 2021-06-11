from main import runner, utils
import heapq
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
import copy
STRIP_THRESHOLD = 0.03

# SIZES_TO_LOOP = [1024, 2048, 4096, 8192]
SIZES_TO_LOOP = [4096, 8192]
MESSAGE_TYPES = ["single"]
MESSAGE_TYPES.extend(["list-2", "list-4", "list-6", "list-8"])
#MESSAGE_TYPES.extend(["list-{}".format(i) for i in range(1, 5)])
#MESSAGE_TYPES.extend(["tree-{}".format(i) for i in range(1, 4)])
RECV_TYPES = ["zero_copy_recv", "copy_to_dma_memory", "copy_out_recv"]
MAX_CLIENT_RATE_PPS = 60000
MAX_NUM_CLIENTS = 7
CLIENT_RATE_INCREMENT = 20000
SERIALIZATION_LIBRARIES = ["cornflakes-dynamic", "cornflakes-fixed",
                           "cornflakes1c-dynamic", "cornflakes1c-fixed"]  # "cornflakes1c-fixed", "protobuf", "capnproto",
# "flatbuffers"]


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
                 recv_type,
                 trial=None):
        """
        Arguments:
        * client_rates: Mapping from {int, int} specifying rates and how many
        clients send at that rate. Total clients cannot exceed macximum clients
        on the machine.
        * size: Total size of the data structure to be echoed (not including
        additional header space the serialization library will use).
        * serialization: Serialization library to use.
        * message_type: Type of data structure to echo.
        * recv_type: [zero_copy_recv, copy_to_dma_memory, copy_out_recv] - What receive
        mode is used. For cornflakes, one of first two must be enabled.
        zero-copy send from zero-copy receive.
        """
        self.client_rates = client_rates
        self.size = size
        self.serialization = serialization
        self.message_type = message_type
        self.recv_mode = recv_type
        self.trial = trial

    def get_size(self):
        return self.size

    def get_serialization(self):
        return self.serialization

    def get_message_type(self):
        return self.message_type

    def get_recv_type(self):
        return self.recv_mode

    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

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

    def get_recv_string(self):
        return self.recv_mode

    def get_size_string(self):
        return "size_{}".format(self.size)

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
            "recv: {}, " \
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_size_string(),
                               self.serialization,
                               self.message_type,
                               self.get_recv_string(),
                               self.get_trial_string())

    def get_serialization_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization / self.message_type / self.get_size_string() / self.get_recv_string() / self.get_client_rate_string()

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
        ret["cornflakes_dir"] = config_yaml["cornflakes_dir"]
        ret["config_file"] = config_yaml["config_file"]
        ret["size"] = "{}".format(self.size)
        ret["library"] = self.serialization
        ret["message"] = self.message_type
        ret["folder"] = str(folder)
        if program == "start_server":
            ret["zero_copy_recv"] = ""
            ret["copy_to_dma_memory"] = ""
            if self.recv_mode == "zero_copy_recv":
                ret["zero_copy_recv"] = " -z"
            elif self.recv_mode == "copy_to_dma_memory":
                ret["copy_to_dma_memory"] = "--copy_to_dma_memory"
        elif program == "start_client":
            ret["zero_copy_recv"] = " -z"  # always have zero_copy_recv on
            ret["copy_to_dma_memory"] = ""
            # calculate client rate
            host_options = self.get_iteration_clients(
                programs_metadata[program]["hosts"])
            rate = self.find_rate(host_options, host)
            ret["rate"] = rate

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
        self.config_yaml = yaml.load(Path(config_yaml).read_text())
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text())

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
                                    total_args.recv_mode)
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
            for trial in range(2):
                # for trial in range(utils.NUM_TRIALS):
                for message_type in MESSAGE_TYPES:
                    for size in SIZES_TO_LOOP:
                        for serialization in SERIALIZATION_LIBRARIES:
                            if size == 8192\
                                and message_type == "tree-5"\
                                and (serialization == "cornflakes-dynamic"
                                     or serialization == "cornflakes-1cdynamic"):
                                continue
                            recv_modes = ["zero_copy_recv"]
                            if (serialization == "cornflakes-dynamic"
                                    or serialization == "cornflakes-fixed"):
                                recv_modes.append("copy_to_dma_memory")
                            else:
                                recv_modes.append("copy_out_recv")
                            for recv_mode in recv_modes:
                                # for client rates:
                                # for now loop over 2 rates and 1-2 machines
                                # do some testing to determine optimal rates
                                client_rates = [[(24000, 1)],
                                                [(48000, 1)],
                                                [(72000, 1)],
                                                [(96000, 1)]]
                                for i in range(2, int(self.config_yaml["max_clients"])):
                                    client_rates.append([(100000, i)])
                                for rate in client_rates:
                                    it = EchoBenchIteration(rate,
                                                            size,
                                                            serialization,
                                                            message_type,
                                                            recv_mode,
                                                            trial=trial)
                                    ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        if namespace.exp_type == "individual":
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
                                choices=SERIALIZATION_LIBRARIES,
                                required=True)
            parser.add_argument("-z", "--recv_mode",
                                dest="recv_mode",
                                choices=RECV_TYPES,
                                required=True)
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "serialization,message_type,size,recv_mode,"\
            "offered_load_pps,offered_load_gbps,"\
            "achieved_load_pps,achieved_load_gbps,"\
            "percent_acheived_rate,total_retries"\
            "avg,median,p99,p999"

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

            host_offered_load = float(run_info["args"]["rate"])
            total_offered_load_pps += host_offered_load
            host_offered_load_gbps = float(utils.get_tput_gbps(host_offered_load,
                                                               iteration.get_size()))
            total_offered_load_gbps += host_offered_load_gbps
            host_pkts_recved = stdout_info["recved"]
            host_total_time = stdout_info["total_time"]
            host_achieved_load = float(
                host_pkts_recved) / float(host_total_time)
            total_achieved_load_pps += host_achieved_load
            host_achieved_load_gbps = float(utils.get_tput_gbps(host_achieved_load,
                                                                iteration.get_size()))
            total_achieved_load_gbps += host_achieved_load_gbps

            # convert to microseconds
            host_p99 = utils.p99_func(latencies) / 1000.0
            host_p999 = utils.p999_func(latencies) / 1000.0
            host_median = utils.median_func(latencies) / 1000.0
            host_avg = utils.mean_func(latencies) / 1000.0

            # add retries
            retries = int(stdout_info["retries"])
            total_retries += retries

            if print_stats:
                utils.info("Client {}: "
                           "offered load: {:.4f} req/s | {:.4f} Gbps, "
                           "achieved load: {:.4f} req/s | {:.4f} Gbps, "
                           "percentage achieved rate: {:.4f}, "
                           "retries: {}, "
                           "avg latency: {: .4f} \u03BCs, p99: {: .4f} \u03BCs, p999:"
                           "{: .4f} \u03BCs, median: {: .4f} \u03BCs".format(
                               host, host_offered_load, host_offered_load_gbps,
                               host_achieved_load, host_achieved_load_gbps,
                               float(host_achieved_load / host_offered_load),
                               retries,
                               host_avg, host_p99, host_p999, host_median))

        sorted_latencies = list(heapq.merge(*client_latency_lists))
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
                                                                      iteration.get_message_type(),
                                                                      iteration.get_size(),
                                                                      iteration.get_recv_string(),
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

    def graph_results(self, folder, logfile):
        utils.warn("Graphing not implemented yet")


def main():
    parser, namespace = runner.get_basic_args()
    echo_bench = EchoBench(
        namespace.exp_config,
        namespace.config)
    echo_bench.execute(parser, namespace)


if __name__ == '__main__':
    main()
