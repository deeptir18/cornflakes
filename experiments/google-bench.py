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
                           "capnproto", "flatbuffers", "protobuf"]

class GoogleIteration(runner.Iteration):
    def __init__(self,
                client_rates,
                total_num_keys,
                key_size,
                max_size,
                max_num_values,
                serialization_library,
                extra_serialization_params,
                num_threads,
                max_bucket = 16384, # TODO: make max bucket configurable
                distribution = "exponential",
                trial = None):
        self.client_rates = client_rates
        self.total_num_keys = total_num_keys
        self.key_size = key_size
        self.max_size = max_size
        self.serialization = serialization_library
        self.extra_serialization_params = extra_serialization_params
        self.max_bucket = max_bucket
        self.num_threads = num_threads
        self.distribution = distribution
        self.max_num_values = max_num_values
        self.trial = trial

    def calculate_iteration_stats(self, local_folder, client_file_list,
            print_stats):
        """
        Analyzes this iteration stats.
        If print_stats is true, prints out values of parameters and values
        of stats.
        Default implementation assumes Rust clients that implement the state
        machine trait.
        Otherwise, writes csv into local_folder/analysis.log.
        Overwritten as offered load pps or calculating gbps doesn't apply for
        twitter trace.
        """
        # TODO: add in offered load stats
        packets_received = 0
        packets_sent = 0
        max_runtime = 0
        size_bucket_histogram = {}
        histogram = utils.Histogram({})
        for filename in client_file_list:
            # format:
            # map 1 = total histogram over all sizes
            # map 2 = per size histogram (summed over these threads)
            # map 3 = int -> thread stats
            yaml_map = yaml.load(Path(filename).read_text(), Loader =
                    yaml.FullLoader)
            total_histogram = utils.Histogram(yaml_map[0])
            histogram.combine(total_histogram)
            for size_bucket_str in yaml_map[1]:
                size_bucket = int(size_bucket_str)
                hist = utils.Histogram(yaml_map[1][size_bucket_str])
                if size_bucket not in size_bucket_histogram:
                    size_bucket_histogram[size_bucket] = hist
                else:
                    size_bucket_histogram[size_bucket].combine(hist)

            threads_map = yaml_map[2]
            for thread, thread_info in threads_map.items():
                packets_sent += thread_info["num_sent"]
                packets_received += thread_info["num_received"]
                thread_runtime = thread_info["runtime"]
                max_runtime = max(thread_runtime, max_runtime)

        # calculate full statistics
        achieved_load_pps = float(packets_received) / max_runtime
        achieved_load_pps_sent = float(packets_sent) / max_runtime
        iteration_params = self.get_iteration_params_values()
        total_count = total_histogram.count

        percent_achieved = float(achieved_load_pps) / float(achieved_load_pps_sent)
        iteration_params["achieved_load_pps"] = achieved_load_pps
        iteration_params["achieved_load_pps_sent"] = achieved_load_pps_sent
        iteration_params["percent_achieved_rate"] = percent_achieved
        iteration_params["avg"] = total_histogram.avg() / float(1000)
        iteration_params["median"] = total_histogram.value_at_quantile(0.50) / float(1000)
        iteration_params["p99"] = total_histogram.value_at_quantile(0.99) / float(1000)
        iteration_params["p999"] = total_histogram.value_at_quantile(0.999) / float(1000)

        format_string_params = ["{{{}}}".format(x) for x in
                self.get_csv_header()]
        format_string = ",".join(format_string_params)
        self.fill_in_buckets(iteration_params, size_bucket_histogram)
        format_string = format_string.format(**iteration_params)
        analysis_path = Path(local_folder) / "analysis.log"
        with open(str(analysis_path), "w") as f:
            f.write(",".join(self.get_csv_header()) + os.linesep)
            f.write(format_string + os.linesep)
            f.close()
        if print_stats:
            print_string = "Experiment results: \n"\
                               "\t- achieved load received: {:.4f} req/s\n"\
                               "\t- achieved load sent: {:.4f} req/s\n"\
                               "\t- percentage achieved rate: {:.4f}\n"\
                               "\t- avg latency: {:.4f}"\
                               " \u03BCs\n\t- median: {:"\
                               ".4f} \u03BCs\n\t- p99: {: .4f}"\
                               " \u03BCs\n\t- p999:"\
                               "{: .4f} \u03BCs".format(
                                       achieved_load_pps,
                                       achieved_load_pps_sent,
                                   percent_achieved,
                                   iteration_params["avg"],
                                   iteration_params["median"],
                                   iteration_params["p99"],
                                   iteration_params["p999"])
            min_bucket = 8
            while min_bucket <= self.max_bucket:
                if min_bucket not in size_bucket_histogram:
                    min_bucket *= 2
                    continue
                print_string += "\n\t- Size {:d}: median: {: .4f} \u03BCs".format(min_bucket, iteration_params["size{}_p50".format(min_bucket)])
                print_string += " & p99: {: .4f} \u03BCs ({:d} reqs or {: .2f} %)".format(iteration_params["size{}_p99".format(min_bucket)],
                        iteration_params["size{}_count".format(min_bucket)],
                        float(iteration_params["size{}_count".format(min_bucket)])
                        / float(total_count))
                min_bucket *= 2
            utils.info(print_string)


    
    def get_bucket_list(self):
        ret = []
        bucket_size = 8
        while bucket_size <= self.max_bucket:
            ret.extend(["size{}_p50".format(bucket_size),
                "size{}_p99".format(bucket_size),
                "size{}_count".format(bucket_size)])
            bucket_size *= 2
        return ret

    def fill_in_buckets(self, iteration_params, size_bucket_histogram):
        bucket_size = 8
        while bucket_size <= self.max_bucket:
            if bucket_size in size_bucket_histogram:
                hist = size_bucket_histogram[bucket_size]
                iteration_params["size{}_p50".format(bucket_size)] = hist.value_at_quantile(0.50) / float(1000)
                iteration_params["size{}_p99".format(bucket_size)] = hist.value_at_quantile(0.99) / float(1000)
                iteration_params["size{}_count".format(bucket_size)] = hist.count
            else:
                iteration_params["size{}_p50".format(bucket_size)] = 0
                iteration_params["size{}_p99".format(bucket_size)] = 0
                iteration_params["size{}_count".format(bucket_size)] = 0
            bucket_size *= 2

    def get_csv_header(self):
        csv_order = self.get_iteration_params()
        csv_order.extend(["achieved_load_pps", "achieved_load_pps_sent",
        "percent_achieved_rate", "avg", "median", "p99", "p999"])
        csv_order.extend(self.get_bucket_list())
        return csv_order


    def __str__(self):
        return "Iteration info: " \
                "client rates: {}, " \
                "total_num_keys: {}," \
                "max_size: {}," \
                "key size: {}," \
                "serialization: {}, "\
                "max_num_values: {}, "\
                "num_threads: {}, "\
                "extra serialization_params: {}, "\
                "distribution: {}, "\
                "trial: {}".format(
                        self.get_client_rate_string(),
                        self.get_total_num_keys_string(),
                        self.get_max_size_string(),
                        self.get_key_size_string(),
                        self.serialization,
                        self.get_max_num_values_string(),
                        self.num_threads,
                        str(self.extra_serialization_params),
                        self.distribution,
                        self.get_trial_string())
    
    def hash(self):
        # hashes every argument EXCEPT for rates
        args = [self.total_num_keys, self.max_size, self.key_size, self.serialization, self.max_num_values,
                self.get_num_clients(), self.num_threads, self.distribution,
                str(self.extra_serialization_params), self.trial]
        return args

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment.
        """
        params = ["serialization", "max_num_values", "total_num_keys", "max_size", "key_size", "num_threads",
                "num_clients", "distribution", "offered_load_pps"]
        params.extend(self.extra_serialization_params.get_iteration_params())
        return params

    def get_iteration_params_values(self):
        offered_load_pps = 0
        for info in self.client_rates:
            rate = info[0]
            num = info[1]
            offered_load_pps += rate * num * self.num_threads
        ret = {
                "total_num_keys": self.total_num_keys,
                "max_size": self.max_size,
                "key_size": self.key_size,
                "offered_load_pps": offered_load_pps,
                "num_threads": self.num_threads,
                "num_clients": self.get_num_clients(),
                "serialization": self.extra_serialization_params.get_serialization_name(),
                "distribution": self.distribution,
                "max_num_values": self.max_num_values
            }
        ret.update(self.extra_serialization_params.get_iteration_params_values())
        return ret
    
    def get_total_num_keys(self):
        return self.total_num_keys
    
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

    def get_num_threads(self):
        return self.num_threads
    
    def get_serialization(self):
        return self.serialization
    
    def get_trial(self):
        return self.trial

    def set_trial(self, trial):
        self.trial = trial

    def get_trial_string(self):
        if self.trial == None:
            utils.error("TRIAL IS NOT SET FOR ITERATION.")
            exit(1)
        return "trial_{}".format(self.trial)

    def get_total_num_keys_string(self):
        return "num_keys_{}".format(self.total_num_keys)

    def get_key_size_string(self):
        return "key_size_{}".format(self.key_size)

    def get_max_num_values_string(self):
        return "max_num_values_{}".format(self.max_num_values)

    def get_max_size_string(self):
        return "max_size_{}".format(self.max_size)

    def get_distribution_string(self):
        return "distribution_{}".format(self.distribution)

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

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization /\
                self.extra_serialization_params.get_subfolder() /\
                self.get_total_num_keys_string() /\
                self.get_max_num_values_string() /\
                self.get_key_size_string() /\
                self.get_max_size_string() /\
                self.get_distribution_string() /\
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
        ret["total_num_keys"] = self.total_num_keys
        ret["key_size"] = self.key_size
        ret["library"] = self.serialization
        ret["client_library"] = self.serialization
        ret["max_size"] = self.max_size
        ret["distribution"]  = self.distribution
        ret["max_num_values"] = self.max_num_values

        self.extra_serialization_params.fill_in_args(ret, program)
        host_type_map = config_yaml["host_types"]
        server_host = host_type_map["server"][0]
        if program == "start_server":
            ret["server_ip"] = config_yaml["hosts"][host]["ip"]
            ret["mode"] = "server"
        elif program == "start_client":
            host_options = self.get_iteration_clients(
                    host_type_map["client"])
            ret["mode"] = "client"
            rate = self.find_rate(host_options, host)
            ret["num_threads"] = self.num_threads
            ret["num_clients"] = len(host_options)
            ret["num_machines"] = self.get_num_clients()
            ret["machine_id"] = self.find_client_id(host_options, host)
            ret["rate"] = rate

            # calculate server host
            ret["server_ip"] =  config_yaml["hosts"][server_host]["ip"]
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret

GoogleExpInfo = collections.namedtuple("GoogleExpInfo", ["total_num_keys",
    "key_size", "distribution", "max_num_values"])

class GoogleBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "GoogleBench"
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
        Returns parsed GoogleExpInfo from exp_string.
        Should be formatted as:
        total_num_keys = {}, key_size = {}, distribution = {}, max_num_values = {}
        """
        try:
            parse_result = parse.parse("total_num_keys = {:d}, key_size = {:d}, distribution = {}, max_num_values = {:d}", exp_string)
            return GoogleExpInfo(parse_result[0], 
                    parse_result[1],
                    parse_result[2],
                    parse_result[3])
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
            extra_serialization_params = runner.ExtraSerializationParameters(total_args.serialization,
                    total_args.buf_mode,
                    total_args.inline_mode,
                    total_args.max_sg_segments,
                    total_args.copy_threshold)
            client_rates = [(total_args.rate, total_args.num_clients)]
            it = GoogleIteration(
                    client_rates,
                    total_args.total_num_keys,
                    total_args.key_size,
                    total_args.max_size,
                    total_args.max_num_values,
                    total_args.serialization,
                    extra_serialization_params,
                    total_args.num_threads,
                    distribution = total_args.distribution,
                    trial = None)
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
            max_size = utils.yaml_get(loop_yaml, "max_size")
            rate_percentages = utils.yaml_get(loop_yaml, "rate_percentages")
            serialization_libraries = utils.yaml_get(loop_yaml,
                    "serialization_libraries")
            max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
            # make it easy to parse different serialization libraries
            serialization_libraries = utils.yaml_get(loop_yaml,
                    "serialization_libraries")
            for trial in range(num_trials):
                for serialization in serialization_libraries:
                    for rate_percentage in rate_percentages:
                        for exp in max_rates_dict:
                            key_size = exp.key_size
                            max_rate = max_rates_dict[exp]
                            rate = int(float(max_rate) * rate_percentage)
                            client_rates = [(rate, num_clients)]
                            extra_serialization_params = runner.ExtraSerializationParameters(serialization)
                            distribution = exp.distribution
                            total_num_keys = exp.total_num_keys
                            max_num_values = exp.max_num_values
                            it = GoogleIteration(
                                client_rates,
                                total_num_keys,
                                key_size,
                                max_size,
                                max_num_values,
                                serialization,
                                extra_serialization_params,
                                num_threads,
                                distribution = distribution,
                                trial = trial)
                            ret.append(it)
            return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        parser.add_argument("-nk", "--total_num_keys",
                            dest="total_num_keys",
                            type = int,
                            default = 1000000)
        parser.add_argument("-ks", "--key_size",
                            dest="key_size",
                            type = int,
                            default = 32,
                            )
        parser.add_argument("-ms", "--max_size",
                            dest="max_size",
                            type = int,
                            default = 8192,
                            )
        parser.add_argument("-mnm", "--max_num_values",
                            dest="max_num_values",
                            type = int,
                            default = 1)
        if namespace.exp_type == "individual":
            parser.add_argument("-nt", "--num_threads",
                                dest="num_threads",
                                type=int,
                                default=1,
                                help="Number of threads to run with")
            parser.add_argument("-r", "--rate",
                                dest="rate",
                                type=int,
                                default=1.0,
                                help="Offered load per thread client")
            parser.add_argument("-nc", "--num_clients",
                                dest="num_clients",
                                type=int,
                                default=1)
            parser.add_argument("-ser", "--serialization",
                                dest="serialization",
                                choices=SERIALIZATION_LIBRARIES,
                                required=True)
            parser.add_argument("-dist", "--distribution",
                                dest="distribution",
                                choices=["exponential", "uniform"],
                                default = "uniform")
            runner.extend_with_serialization_parameters(parser) 
        args = parser.parse_args(namespace=namespace)
        return args

    def get_exp_config(self):
        return self.exp_yaml

    def get_machine_config(self):
        return self.config_yaml

    def exp_post_process_analysis(self, total_args, logfile, new_logfile):
        pass

    def graph_results(self, args, folder, logfile, post_process_logfile):
        loop_yaml = self.get_loop_yaml()
        graphing_groups = utils.yaml_get(loop_yaml, "graphing_groups")
        cornflakes_repo = self.config_yaml["cornflakes_dir"]
        plot_path = Path(folder) / "plots"
        plot_path.mkdir(exist_ok=True)
        full_log = Path(folder) / logfile
        plotting_script = Path(cornflakes_repo) / \
            "experiments" / "plotting_scripts" / "google_kv.R"
        base_args = [str(plotting_script), str(full_log)]
        max_rates_dict = self.parse_max_rates(utils.yaml_get(loop_yaml, "max_rates"))
        for metric in ["p99", "median"]:
            for exp in max_rates_dict:
                total_num_keys = exp.total_num_keys
                distribution = exp.distribution
                max_num_values = exp.max_num_values
                key_size = exp.key_size
                base_plot_path = plot_path /\
                        "max_num_values_{}".format(max_num_values) /\
                        "total_num_keys_{}".format(total_num_keys) /\
                        "key_size_{}".format(key_size) /\
                        "distribution_{}".format(distribution)
                base_plot_path.mkdir(parents = True, exist_ok = True)
                if "baselines" in graphing_groups:
                    pdf = base_plot_path / "baselines_{}.pdf".format(metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "baselines",
                                       str(max_num_values),
                                       str(total_num_keys),
                                       str(key_size),
                                       distribution]
                    print(" ".join(total_plot_args))
                    sh.run(total_plot_args)
                if "cornflakes" in graphing_groups:
                    pdf = base_plot_path / "thresholdvary_{}.pdf".format(metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "cornflakes",
                                       str(max_num_values),
                                       str(total_num_keys),
                                       str(key_size),
                                       distribution]
                    print(" ".join(total_plot_args))
                    sh.run(total_plot_args)


def main():
    parser, namespace = runner.get_basic_args()
    google_bench = GoogleBench(
        namespace.exp_config,
        namespace.config)
    google_bench.execute(parser, namespace)


if __name__ == '__main__':
    main()
