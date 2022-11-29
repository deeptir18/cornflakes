from main import runner, utils
import heapq
from result import Ok
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
STRIP_THRESHOLD = 0.03
SERIALIZATION_LIBRARIES = ["cornflakes-dynamic", "cornflakes1c-dynamic",
                           "capnproto", "flatbuffers", "protobuf", "redis"]


class KVIteration(runner.Iteration):
    def __init__(self,
                 client_rates,
                 avg_size,
                 size_distr,
                 total_num_keys,
                 key_size,
                 zipf,
                 retwis_distribution,
                 serialization,
                 num_threads,
                 extra_serialization_params,
                 trial = None):
        self.avg_size = avg_size
        self.client_rates = client_rates
        self.avg_size = avg_size
        self.size_distr = size_distr
        self.total_num_keys = total_num_keys
        self.key_size = key_size
        self.zipf = zipf
        self.retwis_distribution = retwis_distribution
        self.serialization = serialization
        self.trial = trial
        self.num_threads = num_threads
        self.extra_serialization_params = extra_serialization_params
    def __str__(self):
        return "Iteration info: " \
            "client rates: {}, " \
            "value size distr: {}, " \
            "total num keys: {}, " \
            "key size: {}, "\
            "zipf: {}, " \
            "retwis_distribution: {}, " \
            "serialization: {}, " \
            "num_threads: {}, " \
            "extra serialization_params: {}, "\
            "trial: {}".format(self.get_client_rate_string(),
                               self.get_size_distr_string(),
                               self.get_total_num_keys_string(),
                               self.get_key_size_string(),
                               self.get_zipf_string(),
                               self.retwis_distribution,
                               self.serialization,
                               self.num_threads,
                               str(self.extra_serialization_params),
                               self.get_trial_string())
    def hash(self):
        # hashes every argument EXCEPT for client rates.
        args = [self.size_distr, self.total_num_keys, self.key_size, self.zipf,
                self.serialization, self.num_threads, self.trial,
                self.retwis_distribution,
                str(extra_serialization_params)]

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment.
        """
        params= ["serialization", "size_distr", "avg_size", "total_num_keys",
                "key_size",
                "retwis_distribution",
                "zipf",
                "num_clients", 
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
                "key_size": self.key_size,
                "total_num_keys": self.total_num_keys,
                "retwis_distribution": self.retwis_distribution,
                "zipf": self.zipf,
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "offered_load_pps": offered_load_pps,
                "offered_load_gbps": offered_load_gbps,
            }
        ret.update(self.extra_serialization_params.get_iteration_params_values())
        return ret

    def get_total_num_keys(self):
        self.total_num_keys

    def get_zipf(self):
        self.zipf
    def get_key_size(self):
        self.key_size
    def get_key_size_string(self):
        return "keysize_{}".format(self.key_size)

    def get_retwis_distribution(self):
        self.retwis_distribution

    def get_iteration_avg_message_size(self):
        # this doesn't really make sense for this workload
        return self.avg_size

    def get_num_threads(self):
        return self.num_threads

    def get_size_distr(self):
        return self.size_distr

    def get_serialization(self):
        return self.serialization

    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_total_num_keys_string(self):
        return "total_keys_{}".format(self.total_num_keys)

    def get_size_distr_string(self):
        return "size_distr_{}".format(self.size)

    def get_zipf_string(self):
        return "zipf_{}".format(self.zipf)

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def get_num_threads_string(self):
        return "{}_threads".format(self.num_threads)

    def get_retwis_distribution_string(self):
        return "retwis_distr_{}".format(self.retwis_distribution)

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
                self.get_total_num_keys_string() /\
                self.get_key_size_string() /\
                self.get_zipf_string() /\
                self.get_retwis_distribution_string() /\
                self.get_size_distr_string() /\
            self.get_client_rate_string() /\
            self.get_num_threads_string()

    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def get_program_args(self,
                         host,
                         config_yaml,
                         program,
                         programs_metadata):
        ret = {}
        ret["size_distr"] = "{}".format(self.size_distr)
        ret["total_num_keys"] = "{}".format(self.total_num_keys)
        ret["zipf"] = "{}".format(self.zipf)
        ret["retwis_distribution"] = self.retwis_distribution
        ret["key_size"] = "{}".format(self.key_size)
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

RetwisExpInfo = collections.namedtuple("RetwisExpInfo", ["total_num_keys", "zipf",
    "key_size", "value_distribution", "retwis_distribution"])

class KVBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "RetwisBench"
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

    def parse_exp_info_string(self, exp_string):
        """
        Returns parsed RetwisExpInfo from exp_string.
        Should be formatted as:
        total_num_keys = {}, zipf = {}, key_size = {}, value_distribution = {}, retwis_distribution = {}
        num_values = {}, num_keys = {}, size = {}
        """
        try:
            parse_result = parse.parse("total_num_keys = {:d}, zipf = {:f}, key_size = {:d}, value_distribution = {}, retwis_distribution = {}",
            exp_string)
            return RetwisExpInfo(parse_result[0], parse_result[1], parse_result[2],
                    parse_result[3], parse_result[4])
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
            it = KVIteration(client_rates,
                    utils.parse_cornflakes_size_distr_avg(total_args.size_distribution),
                    total_args.size_distribution,
                    total_args.total_num_keys,
                    total_args.key_size,
                    total_args.zipf,
                    total_args.retwis_distribution,
                    total_args.serialization,
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
            ret = []
            loop_yaml = self.get_loop_yaml()
            # loop over various options
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
                        for kvexp in max_rates_dict:
                            max_rate = max_rates_dict[kvexp]
                            total_num_keys = kvexp.total_num_keys
                            key_size = kvexp.keysize
                            zipf = kvexp.zipf
                            size_distr = kvexp.value_distribution
                            retwis_distribution = kvexp.retwis_distribution
                            size = utils.parse_cornflakes_size_distr_avg(size_distr)
                            rate = int(float(max_rate) *
                                        rate_percentage)
                            client_rates = [(rate, num_clients)]
                            extra_serialization_params = runner.ExtraSerializationParameters(serialization)
                            it = KVIteration(client_rates,
                                            size,
                                            size_distr,
                                            total_num_keys,
                                            key_size,
                                            zipf,
                                            retwis_distribution,
                                            serialization,
                                            num_threads,
                                            extra_serialization_params,
                                            trial = trial)
                            ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        if namespace.exp_type == "individual":
            parser.add_argument("--zipf",
                                type = float,
                                default = .75,
                                help = "Zipf distribution for redis")
            parser.add_argument("--key_size",
                                type = int,
                                default = 64,
                                help = "Key size")
            parser.add_argument("--total_num_keys",
                                type = int,
                                default = 1000000,
                                help = "Total keys in kv store")
            parser.add_argument("--size_distribution",
                                default = "UniformOverSizes-512",
                                help = "Value size distribution")
            parser.add_argument("--retwis_distribution",
                                default = "5-15-30-50",
                                help = "Retwis request distribution.")
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

    def run_summary_analysis(self, out, serialization, retwisexpinfo):
    #RetwisExpInfo = collections.namedtuple("RetwisExpInfo", ["total_num_keys", "zipf",
    #"key_size", "value_distribution", "retwis_distribution"])
        filtered_df = df[(df["serialization"] == serialization) &
                         (df["zipf"] == retwisexpinfo.zipf) &
                         (df["total_num_keys"] == retwisexpinfo.total_num_keys) &
                         (df["key_size"] == retwisexpinfo.key_size) &
                         (df["value_distribution"] == retwisexpinfo.value_distribution) &
                         (df["retwis_distribution"] == retwisexpinfo.retwis_distribution)]
        total_size = int(size * num_values)
        rounded_size = int(size)

        factor_name = f"{num_values} {rounded_size} Byte Values"
        if num_values == 1:
            factor_name = f"{num_values} {rounded_size} Byte Value"
        utils.info(f"Serialization: {serialization}, num_values: {num_values}, size: {size}")

        def ourstd(x):
            return np.std(x, ddof=0)

        # CURRENT KNEE CALCULATION:
        # just find maximum achieved rate across all rates
        clustered_df = filtered_df.groupby(["serialization",
                                            "zipf",
                                            "total_num_keys",
                                            "key_size",
                                            "value_distribution",
                                            "retwis_distribution",
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
        max_achieved_pps = clustered_df["achieved_load_pps_mean"].max()
        max_achieved_gbps = clustered_df["achieved_load_gbps_mean"].max()
        std_achieved_pps = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'achieved_load_pps_sd']
        percent_achieved = clustered_df.loc[clustered_df['achieved_load_pps_mean'].idxmax(),
                                            'percent_achieved_rate']
        std_achieved_gbps = clustered_df.loc[clustered_df['achieved_load_gbps_mean'].idxmax(),
                                             'achieved_load_gbps_sd']
        as_one = False
        out.write(str(serialization) + "," + str(total_num_keys) + "," +
                  str(key_size) + "," +
                  str(zipf) + "," +
                  str(value_distribution) + "," +
                  str(retwis_distribution) + "," +
                  str(max_achieved_pps) + "," +
                  str(max_achieved_gbps) + "," +
                  str(std_achieved_pps) + "," +
                  str(std_achieved_gbps) + "," + str(percent_achieved) + os.linesep)

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        # need to determine knee of the curve for each situation
        # TODO: add post processing based on buffer type
        header_str = "serialization,total_num_keys,key_size,zipf,value_distribution,retwis_distribution,"\
            "maxtputpps,maxtputgbps,maxtputppssd,maxtputgbpssd,percentachieved" + os.linesep
        folder_path = Path(total_args.folder)
        out = open(folder_path / new_logfile, "w")
        df = pd.read_csv(folder_path / logfile)
        out.write(header_str)
        loop_yaml = self.get_loop_yaml()
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
        serialization_libraries = utils.yaml_get(loop_yaml, "serialization_libraries")
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))

        for serialization in serialization_libraries:
            for kvexpinfo in max_rates_dict:
                self.run_summary_analysis(df, out, serialization, kvexpinfo)
        out.close()

    def graph_results(self, args, folder, logfile, post_process_logfile):
        print("GRAPHING TODO")
        pass
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        plotting_script = Path(cornflakes_repo) / \
            "experiments" / "plotting_scripts" / "kv_bench.R"
        base_args = [str(plotting_script), str(full_log)]
        metrics = ["p99", "median"]
        loop_yaml = self.get_loop_yaml()
        post_process_log = Path(folder) / post_process_logfile
        max_rates = self.parse_max_rates(utils.yaml_get(loop_yaml,
            "max_rates"))

        # make total plot
        for metric in metrics:
            for retwisexp in max_rates:
                total_num_keys = retwisexp.total_num_keys
                zipf = retwisexp.zipf
                key_size = retwisexp.keysize
                retwis_distribution = retwisexp.retwis_distribution
                value_distribution = retwisexp.value_distribution
                individual_plot_path = plot_path / \
                    "zipf_{}".format(str(zipf)) /\
                    "retwis_distribution_{}".format(retwis_distribution) /\
                    "key_size_{}".format(str(key_size)) /\
                    "total_keys_{}".format(str(total_num_keys)) / \
                    "value_distribution_{}".format(value_distribution)
                individual_plot_path.mkdir(parents=True, exist_ok=True)
                pdf = individual_plot_path / \
                    "zipf_{}_retwisdistr_{}_keysize_{}_totalkeys_{}_valuedistr_{}_{}.pdf".format(
                            str(zipf),
                            retwis_distribution, 
                            str(key_size),
                            str(total_num_keys),
                            value_distribution, metric)
                total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(post_process_log),
                                       str(pdf),
                                       metric, "individual-retwis",
                                       str(zipf),
                                       retwis_distribution,
                                       str(key_size),
                                       str(total_num_keys),
                                       value_distribution]
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
