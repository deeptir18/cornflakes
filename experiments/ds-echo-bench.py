from main import runner, utils
import heapq
import yaml
from pathlib import Path
import os
import parse
import subprocess as sh
import copy
import time
import pandas as pd
import numpy as np
import collections

ALL_SERIALIZATION_LIBRARIES = [
    "cornflakes-dynamic", "cornflakes1c-dynamic", "flatbuffers", "capnproto",
    "protobuf", "onecopy", "twocopy", "ideal", "manualzerocopy"]


class EchoBenchIteration(runner.Iteration):
    def __init__(self, 
                    client_rates, 
                    size,
                    serialization, 
                    message_type,
                    num_threads,
                    extra_serialization_params,
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
        mode is used. For cornflakes, one of first two must be enabled.
        zero-copy send from zero-copy receive.
        * num_threads: Number of threads to use in iteration.
        * extra_serialization_params: Serialization specific parameters
        (inlining, buffer type).
        """
        self.client_rates = client_rates
        self.size = size
        self.serialization = serialization
        self.message_type = message_type
        self.trial = trial
        self.num_threads = num_threads
        self.extra_serialization_params = extra_serialization_params

    def __str__(self):
        return "Iteration info: client rates: {}, " \
            "value size: {}, " \
            "message type: {}, "\
            "serialization: {}, " \
            "num_threads: {}, " \
            "extra serialization_params: {}, "\
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_size_string(),
                               self.message_type,
                               self.serialization,
                               self.num_threads,
                               str(self.extra_serialization_params),
                               self.get_trial_string())

    def hash(self):
        # hash every argument EXCEPT for client rates
        args = [self.size, self.serialization, self.num_threads, self.trial,
                str(self.extra_serialization_params)]
    def get_iteration_params(self):
        """
        Returns array of parameters for this experiment.
        """
        params = ["serialization", "size", "message_type", "num_threads",
                "num_clients", "offered_load_pps", "offered_load_gbps"]
        params.extend(self.extra_serialization_params.get_iteration_params())
        return params

    def get_iteration_avg_message_size(self):
        return self.size

    def get_iteration_params_values(self):
        offered_load_pps = 0
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            offered_load_pps += rate * num * self.num_threads
        # convert to gbps
        offered_load_gbps = utils.get_tput_gbps(offered_load_pps,
                self.size)
        ret = {
                "serialization": self.serialization,
                "size": self.size,
                "message_type": self.message_type,
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "offered_load_pps": offered_load_pps,
                "offered_load_gbps": offered_load_gbps,
                }
        ret.update(self.extra_serialization_params.get_iteration_params_values())
        return ret
    
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
    
    def get_num_threads_string(self):
        return "{}_threads".format(self.num_threads)

    def get_size_str(self):
        return "size_{}".format(self.size)

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

    def get_size_string(self):
        return "size_{}".format(self.size)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization / self.message_type /\
            self.get_size_string() / self.get_client_rate_string() /\
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
        ret["size"] = "{}".format(self.size)
        ret["message"] = self.message_type
        ret["library"] = self.serialization
        ret["client_library"] = self.serialization
        self.extra_serialization_params.fill_in_args(ret, program)
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        if program == "start_server":
            ret["server_ip"] = config_yaml["hosts"][host]["ip"]
            ret["mode"] = "server"
        elif program == "start_client":
            ret["mode"] = "client"

            # calculate client rate
            host_options = self.get_iteration_clients(
                    host_type_map["client"])
            rate = self.find_rate(host_options, host)
            ret["rate"] = rate
            ret["num_threads"] = self.num_threads
            # calculate server host
            ret["server_ip"] =  config_yaml["hosts"][server_host]["ip"]
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret

EchoInfo = collections.namedtuple("EchoInfo", ["message", "total_size"])

class EchoBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "DSEchoBench"
        self.config_yaml = yaml.load(Path(config_yaml).read_text(),
                Loader=yaml.FullLoader)
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text(),
                Loader=yaml.FullLoader)

    def experiment_name(self):
        return self.exp
    
    def skip_iteration(self, total_args, iteration):
        return False

    def append_to_skip_info(self, total_args, iteration, higher_level_folder):
        return

    def parse_exp_info_string(self, exp_string):
        """
        Returns parsed EchoInfo from exp_string.
        Should be formatted as:
        message = {}, total_size = {}
        """
        try:
            parse_result = parse.parse("message = {}, total_size = {:d}",
                    exp_string)

            return EchoInfo(parse_result[0], parse_result[1])
        except:
            utils.error("Error parsing exp_string: {}".format(exp_string))
            exit(1)

    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > int(self.config_yaml["max_clients"]):
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)
            client_rates = [(total_args.rate, total_args.num_clients)]
            extra_serialization_params = runner.ExtraSerializationParameters(total_args.serialization,
                                                total_args.buf_mode,
                                                total_args.inline_mode,
                                                total_args.max_sg_segments,
                                                total_args.copy_threshold)
            it = EchoBenchIteration(client_rates,
                                    total_args.size,
                                    total_args.serialization,
                                    total_args.message_type,
                                    total_args.num_threads,
                                    extra_serialization_params)
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
            loop_yaml = self.get_loop_yaml()
            num_trials = utils.yaml_get(loop_yaml, "num_trials")
            num_threads = utils.yaml_get(loop_yaml, "num_threads")
            num_clients = utils.yaml_get(loop_yaml, "num_clients")
            rate_percentages = utils.yaml_get(loop_yaml, "rate_percentages")
            serialization_libraries = utils.yaml_get(loop_yaml,
                    "serialization_libraries")
            max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))

            for trial in range(num_trials):
                for serialization in serialization_libraries:
                    for rate_percentage in rate_percentages:
                        for echoexp in max_rates_dict:
                            max_rate = max_rates_dict[echoexp]
                            message_type = echoexp.message
                            size = echoexp.total_size
                            rate = int(float(max_rate) *
                                        rate_percentage)
                            client_rates = [(rate, num_clients)]
                            extra_serialization_params = runner.ExtraSerializationParameters(serialization)
                            it = EchoBenchIteration(client_rates,
                                                        size,
                                                        serialization,
                                                        message_type,
                                                        num_threads,
                                                        extra_serialization_params,
                                                        trial=trial)
                            ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
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
                                choices=["single", "list-1", "list-2", "list-8",
                                    "list-4", "list-16", "tree-1", "tree-2",
                                    "tree-3", "tree-4"],
                                required=True)
            parser.add_argument("-nc", "--num_clients",
                                dest="num_clients",
                                type=int,
                                default=1)
            parser.add_argument("-ser", "--serialization",
                                dest="serialization",
                                choices=ALL_SERIALIZATION_LIBRARIES,
                                required=True)
            # extra serialization related parser arguments
            runner.extend_with_serialization_parameters(parser) 
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

    def run_summary_analysis(self, df, out, size, message_type, serialization):
        filtered_df = df[(df["serialization"] == serialization) &
                         (df["size"] == size) &
                         (df["message_type"] == message_type)]

        num_leaves = utils.parse_num_leaves(message_type)
        factor_name = utils.parse_factor_name(num_leaves, message_type,
                size)
        print(size, message_type, serialization)

        def ourstd(x):
            return np.std(x, ddof=0)

        # CURRENT KNEE CALCULATION:
        # just find maximum achieved rate across all rates
        clustered_df = filtered_df.groupby(["serialization",
                                            "size", "message_type",
                                           "offered_load_pps",
                                            "offered_load_gbps"],
                                           as_index=False).agg(
            achieved_load_pps_median=pd.NamedAgg(column="achieved_load_pps",
                                               aggfunc="median"),
            achieved_load_gbps_median=pd.NamedAgg(column="achieved_load_gbps",
                                                aggfunc="median"),
            percent_achieved_rate=pd.NamedAgg(column="percent_achieved_rate",
                                              aggfunc="median"))

        max_achieved_pps = clustered_df["achieved_load_pps_median"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_median"].max()
        as_one = False
        out.write(str(serialization) + "," + str(message_type) + "," +
                  str(size) + "," +
                  str(factor_name) + "," +
                  str(num_leaves) + "," +
                  str(max_achieved_pps) + "," +
                  str(max_achieved_gbps) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        # TODO: add post processing based on buffer type
        if total_args.loop_mode == "motivation":
            return
        # need to determine: just knee of curve for each situation
        header_str = "serialization,message_type,size,factor_name,num_leaves,"\
            "maxtputpps,maxtputgbps" + os.linesep
        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)
        
        loop_yaml = self.get_loop_yaml()
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
        serialization_libraries = utils.yaml_get(loop_yaml, "serialization_libraries")
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))

        for serialization in serialization_libraries:
            for echoinfo in max_rates_dict:
                total_size = echoinfo.total_size
                message_type = echoinfo.message
                self.run_summary_analysis(
                    df, out, total_size, message_type, serialization)
        out.close()

    def graph_results(self, args, folder, logfile, post_process_logfile):
        # TODO: fix this for the new yaml setup
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        post_process_log = Path(folder) / post_process_logfile
        plotting_script = Path(cornflakes_repo) / \
            "experiments" / "plotting_scripts" / "echo_bench.R"
        base_args = [str(plotting_script), str(full_log)]
        metrics = ["p99", "median"]
        
        loop_yaml = self.get_loop_yaml()
        post_process_log = Path(folder) / post_process_logfile
        max_rates = self.parse_max_rates(utils.yaml_get(loop_yaml,
            "max_rates"))

        if "summary_sizes" in loop_yaml:
            x_axis_label = utils.yaml_get(loop_yaml, "summary_x_axis")
            summary_sizes = utils.yaml_get(loop_yaml, "summary_sizes")
            for total_size in summary_sizes:
                pdf = plot_path /\
                    "summary_{}_tput.pdf".format(total_size)
                total_plot_args = [str(plotting_script),
                                    str(full_log),
                                    str(post_process_log),
                                    str(pdf),
                                    "foo",
                                    "list-compare",
                                    str(total_size),
                                    x_axis_label,
                                ]
                print(" ".join(total_plot_args))
                sh.run(total_plot_args)
                


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

        
            for metric in metrics:
                for echoexp in max_rates:
                    size = echoexp.total_size
                    message_type = echoexp.message
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
