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
STRIP_THRESHOLD = 0.03
NUM_CLIENTS = 2
NUM_THREADS = 4
rates = [6250, 10000, 12500, 18750, 25000, 30000, 35000, 45000, 50000, 55000,
         60000, 65000, 75000, 85000, 95000, 105000, 115000, 125000, 130000,
         135000]

# SIZES_TO_LOOP = [256, 512, 1024, 2048, 4096]
SIZES_TO_LOOP = [4096, 4096]
NUM_VALUES_TO_LOOP = [1, 2, 4, 8]
NUM_KEYS_TO_LOOP = [1, 2, 4, 8]
SERIALIZATION_LIBRARIES = ["cornflakes-dynamic", "cornflakes1c-dynamic",
                           "capnproto", "flatbuffers", "protobuf"]


# TODO: for now. for baselines inline packet header, for cornflakes, inline
# object header
class KVIteration(runner.Iteration):
    def __init__(self,
                 client_rates,
                 avg_size,
                 size_distr,
                 num_keys,
                 num_values,
                 serialization,
                 load_trace,
                 access_trace,
                 num_threads,
                 extra_serialization_params,
                 trial=None):
        self.avg_size = avg_size
        self.client_rates = client_rates
        self.size_distr = size_distr
        self.num_keys = num_keys
        self.serialization = serialization
        self.num_threads = num_threads
        self.num_values = num_values
        self.trial = trial
        self.load_trace = load_trace
        self.access_trace = access_trace
        self.extra_serialization_params = extra_serialization_params

    def __str__(self):
        return "Iteration info: client rates: {}, " \
            "value size distr: {}, " \
            "num keys: {}, " \
            "num values: {}, " \
            "serialization: {}, " \
            "num_threads: {}, " \
            "extra serialization_params: {}, "\
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_size_distr_string(),
                               self.get_num_keys_string(),
                               self.get_num_values_string(),
                               self.serialization,
                               self.num_threads,
                               str(self.extra_serialization_params),
                               self.get_trial_string())
    def hash(self):
        # hashes every argument EXCEPT for client rates.
        args = [self.size_distr, self.num_keys, self.serialization,
                self.num_threads, self.num_values,  self.trial, slef.load_trace,
                self.access_trace, str(self.extra_serialization_params)]

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment.
        """
        params= ["serialization", "size_distr", "avg_size", "num_keys",
                "num_values", "num_threads",
                "num_clients", "load_trace", "access_trace",
                "offered_load_pps", "offered_load_gbps"]
        params.extend(self.extra_serialization_params.get_iteration_params())
        return params

    def get_iteration_params_values(self):
        offered_load_pps = 0
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            offered_load_pps += rate * num * self.num_threads
        # convert to gbps
        offered_load_gbps = utils.get_tput_gbps(offered_load_pps,
                self.get_iteration_avg_message_size())
        ret = {
                "serialization": self.serialization,
                "size_distr": self.size_distr,
                "avg_size": self.avg_size,
                "num_keys": self.num_keys,
                "num_values": self.num_values,
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "load_trace": self.load_trace,
                "access_trace": self.access_trace,
                "offered_load_pps": offered_load_pps,
                "offered_load_gbps": offered_load_gbps,
            }
        ret.update(self.extra_serialization_params.get_iteration_params_values())
        return ret

    def get_iteration_avg_message_size(self):
        return self.avg_size * self.num_values

    def get_num_threads(self):
        return self.num_threads

    def get_size_distr(self):
        return self.size_distr

    def get_serialization(self):
        return self.serialization

    def get_num_values(self):
        return self.num_values

    def get_num_keys(self):
        return self.num_keys

    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_num_values_string(self):
        return "values_{}".format(self.num_values)

    def get_num_keys_string(self):
        return "keys_{}".format(self.num_keys)

    def get_size_distr_string(self):
        return "size_distr_{}".format(self.size)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

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

    def get_num_clients(self):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return total_hosts

    def get_iteration_clients(self, possible_hosts):
        total_hosts = 0
        for i in self.client_rates:
            total_hosts += i[1]
        return possible_hosts[0:total_hosts]

    def find_client_id(self, client_options, host):
        # TODO: what if this doesn't work
        return client_options.index(host)

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

    def get_size_distr_string(self):
        return "size_{}".format(self.size_distr)

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization /\
                self.extra_serialization_params.get_subfolder() /\
                self.get_size_distr_string() /\
            self.get_num_keys_string() /\
            self.get_num_values_string() / self.get_client_rate_string() /\
            self.get_num_threads_string()

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
                         host,
                         config_yaml,
                         program,
                         programs_metadata):
        ret = {}
        ret["size_distr"] = "{}".format(self.size_distr)
        ret["num_keys"] = "{}".format(self.num_keys)
        ret["num_values"] = "{}".format(self.num_values)
        ret["library"] = self.serialization
        ret["client_library"] = self.serialization

        self.extra_serialization_params.fill_in_args(ret, program)
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        if program == "start_server":
            ret["trace"] = self.load_trace
            ret["server_ip"] = config_yaml["hosts"][host]["ip"]
            ret["mode"] = "server"
        elif program == "start_client":
            ret["queries"] = self.access_trace
            ret["mode"] = "client"

            # calculate client rate
            host_options = self.get_iteration_clients(
                    host_type_map["client"])
            rate = self.find_rate(host_options, host)
            ret["rate"] = rate
            ret["num_threads"] = self.num_threads
            ret["num_clients"] = len(host_options)
            ret["num_machines"] = self.get_num_clients()
            ret["machine_id"] = self.find_client_id(host_options, host)

            # calculate server host
            ret["server_ip"] =  config_yaml["hosts"][server_host]["ip"]
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret


class KVBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "KVBench"
        self.exp_yaml = yaml.load(Path(exp_yaml).read_text(),
                Loader=yaml.FullLoader)
        self.config_yaml = yaml.load(Path(config_yaml).read_text(),
                Loader=yaml.FullLoader)

    def experiment_name(self):
        return self.exp

    def skip_iteration(self, total_args, iteration):
        return False

    def append_to_skip_info(self, total_args, iteration, higher_level_folder):
        return

    def get_iterations(self, total_args):
        if total_args.exp_type == "individual":
            if total_args.num_clients > int(self.config_yaml["max_clients"]):
                utils.error("Cannot have {} clients, greater than max {}"
                            .format(total_args.num_clients,
                                    self.config_yaml["max_clients"]))
                exit(1)
            client_rates = [(total_args.rate, total_args.num_clients)]
            value_size = int(total_args.size / total_args.num_values)
            size_distr = "UniformOverSizes-{}".format(value_size)
            extra_serialization_params = runner.ExtraSerializationParameters(total_args.serialization,
                    total_args.buf_mode,
                    total_args.inline_mode,
                    total_args.max_sg_segments,
                    total_args.copy_threshold)
            it = KVIteration(client_rates,
                             value_size,
                             size_distr,
                             total_args.num_keys,
                             total_args.num_values,
                             total_args.serialization,
                             total_args.load_trace,
                             total_args.access_trace,
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
            # loop over various options
            ret = []
            for trial in range(utils.NUM_TRIALS):
                for serialization in SERIALIZATION_LIBRARIES:
                    for rate in rates:
                        client_rates = [(rate, NUM_CLIENTS)]
                        num_threads = NUM_THREADS
                        for total_size in SIZES_TO_LOOP:
                            for num_values in NUM_VALUES_TO_LOOP:
                                for num_keys in NUM_KEYS_TO_LOOP:
                                    if num_keys != num_values:
                                        if num_keys != 1:
                                            continue
                                        size = int(total_size / num_values)
                                        size_str = "UniformOverSizes-{}".format(
                                            size)
                                        it = KVIteration(client_rates,
                                                         size,
                                                         size_str,
                                                         num_keys,
                                                         num_values,
                                                         serialization,
                                                         total_args.load_trace,
                                                         total_args.access_trace,
                                                         num_threads,
                                                         trial=trial)
                                        ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        parser.add_argument("-lt", "--load_trace",
                            dest="load_trace",
                            required=True)
        parser.add_argument("-qt", "--access_trace",
                            dest="access_trace",
                            required=True)
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
                                type=int,
                                help="Total message size.",
                                required=True)
            parser.add_argument("-nk", "--num_keys",
                                dest="num_keys",
                                type=int,
                                default=1,
                                help="Number of keys")
            parser.add_argument("-nv", "--num_values",
                                dest="num_values",
                                type=int,
                                default=1,
                                help="Number of values to batch together")
            parser.add_argument("-nc", "--num_clients",
                                dest="num_clients",
                                type=int,
                                default=1)
            parser.add_argument("-ser", "--serialization",
                                dest="serialization",
                                choices=SERIALIZATION_LIBRARIES,
                                required=True)
            runner.extend_with_serialization_parameters(parser) 
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def get_logfile_header(self):
        return "serialization,value_size,num_keys,num_values,"\
            "offered_load_pps,offered_load_gbps,"\
            "achieved_load_pps,achieved_load_gbps,"\
            "percent_achieved_rate,total_retries,"\
            "avg,median,p99,p999"

    def run_summary_analysis(self, df, out, serialization, num_values, size):
        filtered_df = df[(df["serialization"] == serialization) &
                         (df["value_size"] == size) &
                         (df["num_values"] == num_values)]
        total_size = int(size * num_values)
        print(serialization, num_values, size)

        def ourstd(x):
            return np.std(x, ddof=0)

        # CURRENT KNEE CALCULATION:
        # just find maximum achieved rate across all rates
        # group by array size, num segments, segment size,  # average
        clustered_df = filtered_df.groupby(["serialization",
                                            "value_size", "num_values",
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
        clustered_df = clustered_df[clustered_df["percent_achieved_rate"] >=
                                    0.95]

        max_achieved_pps = clustered_df["achieved_load_pps_mean"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_mean"].max()
        std_achieved_pps = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'achieved_load_pps_sd']
        percent_achieved = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'percent_achieved_rate']
        std_achieved_gbps = clustered_df.loc[clustered_df['achieved_load_gbps_mean'].idxmax(),
                                             'achieved_load_gbps_sd']
        as_one = False
        out.write(str(serialization) + "," + str(total_size) + "," +
                  str(num_values) + "," +
                  str(max_achieved_pps) + "," +
                  str(max_achieved_gbps) + "," +
                  str(std_achieved_pps) + "," +
                  str(std_achieved_gbps) + "," + str(percent_achieved) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        # need to determine knee of the curve for each situation
        header_str = "serialization,total_size,num_values,"\
            "maxtputpps,maxtputgbps,maxtputppssd,maxtputgbpssd,percentachieved" + os.linesep
        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)

        for serialization in SERIALIZATION_LIBRARIES:
            for num_values in NUM_VALUES_TO_LOOP:
                for total_size in SIZES_TO_LOOP:
                    value_size = int(total_size / num_values)
                    self.run_summary_analysis(df, out, serialization,
                                              num_values, value_size)
        out.close()

    def graph_results(self, args, folder, logfile, post_process_logfile):
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        plotting_script = Path(cornflakes_repo) / \
            "experiments" / "plotting_scripts" / "kv_bench.R"
        base_args = [str(plotting_script), str(full_log)]
        metrics = ["p99", "median"]
        post_process_log = Path(folder) / post_process_logfile

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

        # make summary plots
        for total_size in SIZES_TO_LOOP:
            individual_plot_path = plot_path
            individual_plot_path.mkdir(parents=True, exist_ok=True)
            pdf = individual_plot_path /\
                "summary_{}_tput.pdf".format(total_size)
            total_plot_args = [str(plotting_script),
                               str(full_log),
                               str(post_process_log),
                               str(pdf),
                               "foo",
                               "summary",
                               str(total_size)
                               ]
            print(" ".join(total_plot_args))
            sh.run(total_plot_args)

        # make individual plots
        for metric in metrics:
            for value_size in SIZES_TO_LOOP:
                for batch_size in NUM_VALUES_TO_LOOP:
                    individual_plot_path = plot_path / \
                        "size_{}".format(value_size) / \
                        "batch_{}".format(batch_size)
                    individual_plot_path.mkdir(parents=True, exist_ok=True)
                    pdf = individual_plot_path / \
                        "size_{}_batch_{}_{}.pdf".format(value_size, batch_size,
                                                         metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(post_process_log),
                                       str(pdf),
                                       metric, "individual", str(value_size),
                                       str(batch_size)]
                    print(" ".join(total_plot_args))

                    sh.run(total_plot_args)


def main():
    parser, namespace = runner.get_basic_args()
    kv_bench = KVBench(
        namespace.exp_config,
        namespace.config)
    kv_bench.execute(parser, namespace)


if __name__ == '__main__':
    main()
