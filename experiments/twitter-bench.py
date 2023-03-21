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

class TwitterIteration(runner.Iteration):
    def __init__(self,
                 trace,
                 speed_factor,
                 min_num_keys,
                 serialization,
                 num_clients,
                 num_threads,
                 extra_serialization_params,
                 value_size = 0, # if it is not 0, pass in value size
                 ignore_sets = False,
                 distribution = "exponential",
                 max_bucket = 16384, # TODO: make max bucket configurable
                 trial = None):
        self.trace = trace
        self.value_size = value_size
        self.ignore_sets = ignore_sets
        self.distribution = distribution
        self.speed_factor = speed_factor
        self.min_num_keys = min_num_keys
        self.serialization = serialization
        self.num_clients = num_clients
        self.num_threads = num_threads
        self.extra_serialization_params = extra_serialization_params
        self.max_bucket = max_bucket
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
        self.fill_in_buckets(iteration_params, size_bucket_histogram, max_runtime)
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
                print_string += "\n\t- Size {:d}: median: {: .4f} \u03BCs".format(min_bucket, iteration_params["size{}_median".format(min_bucket)])
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
            ret.extend(["size{}_median".format(bucket_size),
                "size{}_p99".format(bucket_size),
                "size{}_count".format(bucket_size),
                "size{}_pps".format(bucket_size)])
            bucket_size *= 2
        return ret

    def fill_in_buckets(self, iteration_params, size_bucket_histogram,
            max_runtime):
        bucket_size = 8
        while bucket_size <= self.max_bucket:
            if bucket_size in size_bucket_histogram:
                hist = size_bucket_histogram[bucket_size]
                iteration_params["size{}_median".format(bucket_size)] = hist.value_at_quantile(0.50) / float(1000)
                iteration_params["size{}_p99".format(bucket_size)] = hist.value_at_quantile(0.99) / float(1000)
                iteration_params["size{}_count".format(bucket_size)] = hist.count
                iteration_params["size{}_pps".format(bucket_size)] =  float(hist.count) / max_runtime
            else:
                iteration_params["size{}_median".format(bucket_size)] = 0
                iteration_params["size{}_p99".format(bucket_size)] = 0
                iteration_params["size{}_count".format(bucket_size)] = 0
                iteration_params["size{}_pps".format(bucket_size)] =  0
            bucket_size *= 2

    def get_csv_header(self):
        csv_order = self.get_iteration_params()
        csv_order.extend(["achieved_load_pps", "achieved_load_pps_sent",
        "percent_achieved_rate", "avg", "median", "p99", "p999"])
        csv_order.extend(self.get_bucket_list())
        return csv_order


    def __str__(self):
        return "Iteration info: " \
                "trace: {}, "\
                "speed_factor: {}, " \
                "value_size: {}, " \
                "ignore_sets: {}, "\
                "distribution: {}, "\
                "min_num_keys: {}, " \
                "serialization: {}, "\
                "num_clients: {}, "\
                "num_threads: {}, "\
                "extra serialization_params: {}, "\
                "trial: {}".format(
                            self.get_trace(),
                            self.get_speed_factor_string(),
                            self.get_value_size_string(),
                            self.get_ignore_sets_string(),
                            self.distribution,
                            self.get_min_num_keys_string(),
                            self.serialization,
                            self.get_num_clients_string(),
                            self.get_num_threads_string(),
                            str(self.extra_serialization_params),
                            self.get_trial_string())
    def hash(self):
        # hashes every argument EXCEPT for speed factor.
        args = [self.trace, self.value_size, self.ignore_sets,
                self.distribution, self.min_num_keys, self.serialization,
                self.num_clients, self.num_threads,
                str(self.extra_serialization_params), self.trial]
        return args

    def get_iteration_params(self):
        """
        Returns an array of parameters for this experiment.
        """
        params = ["trace", "min_num_keys", "serialization", "num_clients",
        "num_threads", "speed_factor"]
        params.extend(self.extra_serialization_params.get_iteration_params())
        return params

    def get_iteration_params_values(self):
        ret = {
                "trace": self.trace,
                "min_num_keys": self.min_num_keys,
                "value_size": self.value_size,
                "ignore_sets": self.ignore_sets,
                "distribution": self.distribution,
                "serialization":
                self.extra_serialization_params.get_serialization_name(),
                "num_clients": self.num_clients,
                "num_threads": self.num_threads,
                "speed_factor": self.speed_factor,
            }
        ret.update(self.extra_serialization_params.get_iteration_params_values())
        return ret

    
    def get_trace(self):
        return self.trace
    
    def get_speed_factor(self):
        return self.speed_factor
    
    def get_min_num_keys(self):
        return self.min_num_keys
    
    def get_num_clients(self):
        return self.num_clients
    
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

    def get_value_size_string(self):
        if self.value_size == 0:
            return "value_size_twitter"
        else:
            return "value_size_{}".format(self.value_size)
    def get_ignore_sets_string(self):
        return "ignore_sets_{}".format(self.ignore_sets)

    def get_distribution_string(self):
        return "distribution_{}".format(self.distribution)

    def get_num_threads_string(self):
        return "{}_threads".format(self.num_threads)

    def get_num_clients_string(self):
        return "{}_clients".format(self.num_clients)

    def get_speed_factor_string(self):
        return "speed_factor_{}".format(self.speed_factor)

    def get_min_num_keys_string(self):
        return "min_num_keys_{}".format(self.min_num_keys)

    def get_iteration_clients(self, possible_hosts):
        return possible_hosts[0:self.num_clients]

    def get_parent_folder(self, high_level_folder):
        path = Path(high_level_folder)
        return path / self.serialization /\
                self.extra_serialization_params.get_subfolder() /\
                self.get_value_size_string() /\
                self.get_ignore_sets_string() /\
                self.get_distribution_string() /\
                self.get_min_num_keys_string() /\
                self.get_speed_factor_string() /\
            self.get_num_clients_string() /\
            self.get_num_threads_string()

    def get_folder_name(self, high_level_folder):
        return self.get_parent_folder(high_level_folder) / self.get_trial_string()

    def get_program_args(self,
                         host,
                         config_yaml,
                         program,
                         programs_metadata):
        ret = {}
        ret["trace_file"] = self.trace
        ret["server_keys"] = self.min_num_keys
        ret["library"] = self.serialization
        ret["client_library"] = self.serialization

        if self.value_size != 0:
            ret["value_size_str"] = " --value_size {}".format(self.value_size)
        else:
            ret["value_size_str"] = ""
        if self.ignore_sets:
            ret["ignore_sets_str"] = " --ignore_sets"
        else:
            ret["ignore_sets_str"] = ""

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
            ret["speed_factor"] = str(self.speed_factor)
            ret["num_threads"] = self.num_threads
            ret["num_clients"] = len(host_options)
            ret["num_machines"] = self.get_num_clients()
            ret["machine_id"] = self.find_client_id(host_options, host)

            # calculate server host
            ret["server_ip"] =  config_yaml["hosts"][server_host]["ip"]
            ret["client_ip"] = config_yaml["hosts"][host]["ip"]
            ret["distribution"] = self.distribution
        else:
            utils.error("Unknown program name: {}".format(program))
            exit(1)
        return ret

TwitterExpInfo = collections.namedtuple("TwitterExpInfo", ["value_size", "ignore_sets", "distribution"])

class TwitterBench(runner.Experiment):
    def __init__(self, exp_yaml, config_yaml):
        self.exp = "TwitterBench"
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
        Returns parsed TwitterExpInfo from exp_string.
        Should be formatted as:
        value_size = {}, ignore_sets = {1|0}, distribution = {exponential|uniform}
        """
        try:
            parse_result = parse.parse("value_size = {:d}, ignore_sets = {:d}, distribution = {}", exp_string)
            ignore_sets = False
            if parse_result[1] == 1:
                ignore_sets = True
            return TwitterExpInfo(
                    parse_result[0], 
                    ignore_sets,
                    parse_result[2])
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
            it = TwitterIteration(
                    total_args.trace,
                    total_args.speed_factor,
                    total_args.min_num_keys,
                    extra_serialization_params.get_serialization(),
                    total_args.num_clients,
                    total_args.num_threads,
                    extra_serialization_params,
                    value_size = total_args.value_size,
                    ignore_sets = total_args.ignore_sets,
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
            speed_factors = utils.yaml_get(loop_yaml, "speed_factors")
            min_num_keys_vec = utils.yaml_get(loop_yaml, "min_num_keys")
            exp_infos = utils.yaml_get(loop_yaml, "configurations")
            configs = [self.parse_exp_info_string(c) for c in exp_infos]
            # make it easy to parse different serialization libraries
            serialization_libraries = utils.yaml_get(loop_yaml,
                    "serialization_libraries")
            for trial in range(num_trials):
                for serialization in serialization_libraries:
                    for speed_factor in speed_factors:
                        for min_num_keys in min_num_keys_vec:
                            
                            extra_serialization_params = runner.ExtraSerializationParameters(serialization)
                            for config in configs:
                                it = TwitterIteration(
                                    total_args.trace,
                                    speed_factor,
                                    min_num_keys,
                                    extra_serialization_params.get_serialization(),
                                    num_clients,
                                    num_threads,
                                    extra_serialization_params,
                                    config.value_size,
                                    config.ignore_sets,
                                    config.distribution,
                                    trial = trial)
                                ret.append(it)
                return ret

    def add_specific_args(self, parser, namespace):
        parser.add_argument("-l", "--logfile",
                            help="logfile name",
                            default="latencies.log")
        parser.add_argument("-tt", "--trace",
                            dest="trace",
                            required=True)
        if namespace.exp_type == "individual":
            parser.add_argument("-nt", "--num_threads",
                                dest="num_threads",
                                type=int,
                                default=1,
                                help="Number of threads to run with")
            parser.add_argument("-sf", "--speed_factor",
                                dest="speed_factor",
                                type=float,
                                default=1.0,
                                help="Speed factor to speed up twitter traces.")
            parser.add_argument("-vs", "--value_size",
                                dest = "value_size",
                                type = int,
                                default = 0)
            parser.add_argument("-is", "--ignore_sets",
                                dest = "ignore_sets",
                                action = 'store_true')
            parser.add_argument("-dist", "--distribution",
                                dest = "distribution",
                                choices = ["exponential", "uniform"])
            parser.add_argument("-mnk", "--min_num_keys",
                                dest = "min_num_keys",
                                type = int,
                                default = 1000000)
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

    def run_summary_analysis(self, df, out, serialization, num_values, num_keys, size):
        pass

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
            "experiments" / "plotting_scripts" / "varied_size_kv.R"
        base_args = [str(plotting_script), str(full_log)]

        exp_infos = utils.yaml_get(loop_yaml, "configurations")
        configs = [self.parse_exp_info_string(c) for c in exp_infos]

        for config in configs:
            base_plot_path = plot_path /\
                    "value_size_{}".format(config.value_size) /\
                    "ignore_sets_{}".format(config.ignore_sets) /\
                    "distribution_{}".format(config.distribution)
            for metric in ["p99", "median"]:
                if "baselines" in graphing_groups:
                    pdf = base_plot_path / "baselines_{}.pdf".format(metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "baselines"]
                    print(" ".join(total_plot_args))
                    sh.run(total_plot_args)
                if "cornflakes" in graphing_groups:
                    pdf = base_plot_path / "thresholdvary_{}.pdf".format(metric)
                    total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "cornflakes"]
                    print(" ".join(total_plot_args))
                    sh.run(total_plot_args)


                # debug by size
                min_bucket = 8
                while min_bucket < 16384:
                    min_bucket = min_bucket * 2
                    metric_subset = "size{}_{}".format(min_bucket, metric)
                    load_subset = "size{}_pps".format(min_bucket)
                    individual_plot_path = base_plot_path / "size{}".format(min_bucket)
                    individual_plot_path.mkdir(exist_ok=True)
                    if "baselines" in graphing_groups:
                        pdf = individual_plot_path / "baselines_size_{}_{}.pdf".format(min_bucket, metric)
                        total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "baselines",
                                       metric_subset,
                                       load_subset]
                        print(" ".join(total_plot_args))
                        sh.run(total_plot_args)
                    if "cornflakes" in graphing_groups:
                        pdf = individual_plot_path / "thresholdvary_size_{}_{}.pdf".format(min_bucket, metric)
                        total_plot_args = [str(plotting_script),
                                       str(full_log),
                                       str(pdf),
                                       metric,
                                       "cornflakes",
                                       metric_subset,
                                       load_subset]
                        print(" ".join(total_plot_args))
                        sh.run(total_plot_args)



def main():
    parser, namespace = runner.get_basic_args()
    twitter_bench = TwitterBench(
        namespace.exp_config,
        namespace.config)
    twitter_bench.execute(parser, namespace)


if __name__ == '__main__':
    main()
