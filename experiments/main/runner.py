#!/usr/bin/python3
import shutil
import abc
import yaml
import argparse
import os
from main import utils
from pathlib import Path
import multiprocessing as mp
import subprocess as sh
import git
from collections import defaultdict
import time
import copy
from tqdm import tqdm
from main import connection
import agenda
import threading
import pandas as pd

def extend_with_serialization_parameters(parser):
    parser.add_argument("-pbt", "--push_buf_type",
                                dest="buf_mode",
                                choices=["singlebuf",
                                    "arenaorderedsga", "object", "echo",
                                    "hybridobject", "hybridarenaobject",
                                    "hybridarenasga"],
                                required=True)
    parser.add_argument("-inline", "--inline_mode",
                                dest="inline_mode",
                                choices=["nothing", "packetheader",
                                    "objectheader"],
                                required=True)
    parser.add_argument("-ct", "--copy_threshold",
                                dest="copy_threshold")
    parser.add_argument("-maxseg", "--max_sg_segments",
                                dest="max_sg_segments",
                                default=1,
                                type=int)
    return parser

class ExtraSerializationParameters(object):
    def __init__(self,
                serialization,
                buf_mode = None,
                inline_mode = None,
                max_sg_segments = None,
                copy_threshold = None):
        self.serialization_name = serialization
        self.serialization = serialization
        if "cornflakes-dynamic" in serialization and serialization != "cornflakes-dynamic":
            split = serialization.split("-")
            copy_threshold_parsed = int(split[2])
            copy_threshold = copy_threshold_parsed
            if len(split) > 3:
                buf_mode = split[3] # e.g. could be cornflakes-dynamic-512-hybridarenasga
            self.serialization = "cornflakes-dynamic"

        # extra parameters
        if copy_threshold != None:
            self.copy_threshold = copy_threshold
        else:
            if self.serialization == "cornflakes1c-dynamic":
                self.copy_threshold = "1000000000" # basically infinity
            else:
                self.copy_threshold = 0
        

        if buf_mode != None:
            self.buf_mode = buf_mode
        else:
            if self.serialization == "cornflakes-dynamic" or self.serialization == "cornflakes1c-dynamic":
                self.buf_mode = "hybridarenaobject"
            elif self.serialization == "ideal"\
                    or self.serialization == "manualzerocopy"\
                    or self.serialization == "onecopy"\
                    or self.serialization == "twocopy":
                self.buf_mode = "echo"
            else:
                self.buf_mode = "singlebuf"

        if inline_mode != None:
            self.inline_mode = inline_mode
        else:
            self.inline_mode = "nothing"


        if max_sg_segments != None:
            self.max_sg_segments = max_sg_segments
        else:
            if self.serialization == "cornflakes-dynamic":
                self.max_sg_segments = 32
            else:
                self.max_sg_segments = 1

    def get_serialization(self):
        return self.serialization

    def get_serialization_name(self):
        return self.serialization_name

    def fill_in_args(self, ret, program_name):
        ret["library"] = self.serialization
        ret["inline_mode"] = self.inline_mode
        ret["push_buf_type"] = self.buf_mode
        ret["copy_threshold"] = self.copy_threshold
        ret["max_sg_segments"] = self.max_sg_segments
            
        if ret["library"] == "cornflakes-dynamic":
            ret["client_library"] = "cornflakes1c-dynamic"

        if program_name == "start_client":
            ret["inline_mode"] = "nothing"
            ret["push_buf_type"] = "singlebuf"
            ret["copy_threshold"] = "infinity"
            ret["max_sg_segments"] = 1

    def __str__(self):
        return "inline_mode: {}, buf_type: {}, max_sg_segments: {}, copy_threshold: {}".format(self.inline_mode, 
                self.buf_mode,
                self.max_sg_segments, 
                self.copy_threshold)

    def get_iteration_params(self):
        return ["buf_mode", 
                "inline_mode", 
                "max_sg_segments",
                "copy_threshold"]
    def get_iteration_params_values(self):
        return {
            "buf_mode": self.buf_mode,
            "inline_mode": self.inline_mode,
            "max_sg_segments": self.max_sg_segments,
            "copy_threshold": self.copy_threshold,
            }
    def get_buf_mode_str(self):
        return "bufmode_{}".format(self.buf_mode)
    def get_inline_mode_str(self):
        return "inlinemode_{}".format(self.inline_mode)
    def get_max_sg_segments_str(self):
        return "maxsgsegs_{}".format(self.max_sg_segments)
    def get_copy_threshold_str(self):
        return "copythreshold_{}".format(self.copy_threshold)
    def get_subfolder(self):
        return Path(self.get_buf_mode_str()) /\
                    self.get_inline_mode_str() /\
                    self.get_max_sg_segments_str() /\
                    self.get_copy_threshold_str()

class UserNameSpace(object):
    pass


def get_basic_parser():
    parser = argparse.ArgumentParser(
        description='Basic Experiment ArgumentParser',
        conflict_handler='resolve')
    return parser


def get_basic_args():
    parser = get_basic_parser()
    parser.add_argument("-e", "--exp",
                        dest='exp_type',
                        required=True,
                        choices=['loop', 'individual'],
                        help="Experiment Type: [loop, individual]")
    parser.add_argument("-f", "--folder",
                        dest="folder",
                        required=True,
                        help="Folder to place results.")
    parser.add_argument("-c", "--config",
                        dest="config",
                        required=True,
                        help="machine/network configuration information.")
    parser.add_argument("-ec", "--exp_config",
                        dest="exp_config",
                        required=True,
                        help="experiment information.")
    parser.add_argument("-lc", "--loop_config",
                        dest="loop_config",
                        help = "Looping information (required for loop experiment)")
    parser.add_argument("-pp", "--pprint",
                        dest="pprint",
                        help="Print out commands that will be run",
                        action="store_true")
    parser.add_argument("-na", "--no_analysis",
                        dest="no_analysis",
                        help="Don't run analysis",
                        action="store_true")
    parser.add_argument("-a", "--analysis",
                        dest="analysis_only",
                        help="Run analysis only",
                        action="store_true")
    parser.add_argument("-ng", "--no_graph",
                        dest="no_graph",
                        help="Don't run graphing",
                        action="store_true")
    parser.add_argument("-g", "--graph_only",
                        dest="graph_only",
                        help="Run graph only",
                        action="store_true")
    parser.add_argument("-perf", "--perf",
                        dest="use_perf",
                        help="Run perf on the server",
                        action="store_true")
    user_namespace = UserNameSpace()
    parser.parse_known_args(namespace=user_namespace)
    return parser, user_namespace


class Experiment(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def experiment_name(self):
        return

    @abc.abstractmethod
    def get_iterations(self, total_args):
        """
        Returns the iterations to loop the experiment over.
        """
        return

    @abc.abstractmethod
    def add_specific_args(self, parser, namespace):
        """
        Returns argparser for this experiment, to be able to run individual
        trials.
        Arguments:
        * parser - ArgumentParser - basic argument parser.
        * namespace - Arguments already there.
        """
        return

    @abc.abstractmethod
    def get_exp_config(self):
        """
        Returns the experiment config yaml,
        which specifies command lines for this experiment.
        """
        return

    @abc.abstractmethod
    def get_machine_config(self):
        """
        Returns the experiment config yaml,
        which specifies each machine's IP and mac address.
        This is the entire machine configuration (ports, mac addresses, server
        IPs).
        """
        return

    @abc.abstractmethod
    def exp_post_process_analysis(self, total_args, summary_logfile, new_logfile):
        """
        Experiment specific post process analysis (calculate "min p99", "knee",
        etc"
        """
        return

    def run_post_process_analysis(self, total_args, logfile=None):
        """
        Runs any calculations for entire set of data generated.
        """
        if logfile is None:
            return

        utils.info("Original log: {}".format(logfile))
        new_logfile = utils.get_postprocess_logfile(logfile)

        self.exp_post_process_analysis(total_args, logfile, new_logfile)

    @abc.abstractmethod
    def skip_iteration(self, total_args, iteration):
        """
        Checks if we can skip the iterations based on the throughouts achieved
        so far
        """
        return

    @abc.abstractmethod
    def append_to_skip_info(self, total_args, iteration, higher_level_folder):
        """Updates skip info with this iteration"""
        return
    
    def parse_max_rates(self, max_rates):
        ret = {}
        for config in max_rates:
            named_tuple = self.parse_exp_info_string(config)
            ret[named_tuple] = int(max_rates[config])
        return ret


    @abc.abstractmethod
    def parse_exp_info_string(self, exp_string):
        """
        Parses string with exp_info of form:
        num_values = 1, num_keys = 1, size = UniformOverSizes-4096
        into a named tuple that is hashable.
        """
        return

    @abc.abstractmethod
    def graph_results(self, total_args, folder, logfile):
        return

    def run_graphing_scripts(self, total_args, folder, logfile=None):
        if logfile is None:
            return
        if logfile is not None:
            # run post process analysis if required
            self.run_post_process_analysis(total_args, logfile)
            post_process_logfile = utils.get_postprocess_logfile(logfile)
            self.graph_results(total_args, folder, logfile,
                               post_process_logfile)

    def run_analysis_loop(self, total_args, iterations, print_stats=False, logfile=None):
        """
        Runs analysis in a loop for each iteration.
        Includes running any graphing.
        If "analysis.log" does not exist in a certain iteration,
        warns script.
        """
        if len(iterations) == 0:
            return
        csv_header = iterations[0].get_csv_header()
        folder_path = Path(total_args.folder)
        if logfile is None:
            return

        df = pd.DataFrame(columns = csv_header)
        for iteration in iterations:
            analysis_path = iteration.get_folder_name(folder_path) /\
                "analysis.log"
            skipped = iteration.get_folder_name(folder_path) /\
                "skipped.log"
            ret = ""
            if (os.path.exists(skipped)):
                continue
            if not(os.path.exists(analysis_path)):
                status = iteration.run(folder_path,
                                   self.get_exp_config(),
                                   self.get_machine_config(),
                                   total_args.pprint,
                                   total_args.use_perf,
                                   print_stats)
                if not status:
                    utils.warn("Analysis for iteration {} did not exist and failed to rerun".format(str(iteration)))
                    continue
            
            #if True:
            if not(os.path.exists(analysis_path)):
                utils.info("Analyzing iteration {}".format(str(iteration)))
                host_type_map = self.get_machine_config()["host_types"]
                program_args_map = iteration.get_program_args_map(self.get_exp_config(),
                        host_type_map,
                        self.get_machine_config(),
                        total_args.folder)
                client_file_list = iteration.get_client_file_list(
                        self.get_exp_config(), 
                        program_args_map, 
                        host_type_map,
                        total_args.folder)
                iteration.calculate_iteration_stats(
                        iteration.get_folder_name(total_args.folder),
                        client_file_list,
                        False)
            
            iteration_df = iteration.read_analysis_log(folder_path)
            df = pd.concat([df, iteration_df], ignore_index = True)
        # write to logfile
        df.to_csv(str(folder_path/logfile))

    def run_iterations(self, total_args, iterations, print_stats=False):
        """
        Loops over the iterations and runs each experiment.
        """
        folder_path = Path(total_args.folder)
        program_metadata = self.get_exp_config()["programs"]
        ct = 0
        skipped = 0
        already_ran = 0
        total = len(iterations)
        start = time.time()
        expected_time = 30 * total / 3600.0
        utils.warn("Expected time to finish: {} hours".format(expected_time))
        for iteration in iterations:
            ct += 1
            iteration_path = iteration.get_folder_name(folder_path)
            analysis_log = Path(iteration_path) / "analysis.log"
            skipped_log = Path(iteration_path) / "skipped.log"
            if os.path.exists(analysis_log) or os.path.exists(skipped_log):
                utils.info("Analysis or skipped log exists for: {}".format(
                    iteration))
                self.append_to_skip_info(total_args, iteration, folder_path)
                already_ran += 1
                continue
            if self.skip_iteration(total_args, iteration):
                utils.info("Skipping iteration  # {} out of {}".format(ct - 1,
                    total))
                utils.info("Already ran {}; {} were skipped".format( already_ran, skipped))

                skipped += 1
                # append a folder to say this iteration was skipped
                if not total_args.pprint:
                    iteration.create_folder(folder_path)
                skipped_log = iteration.get_folder_name(folder_path) /\
                    "skipped.log"
                with open(skipped_log, 'w') as f:
                    pass
                continue
            
            if (ct > 1):
                rate_so_far = (ct - 1)/(time.time() - start)
                left = (total - (ct - 1))
                expected_time_to_finish = (
                    float(left) / (float(rate_so_far))) / 3600.0
                utils.info("Running iteration  # {} out of {}, {} % done with iterations expected time to finish: {} hours; already_ran {}, skipped {}".format(
                    ct - 1, total, (float(ct - 1) /
                                    float(total) * 100.0),
                    expected_time_to_finish, already_ran, skipped))

            utils.debug("Running iteration: ", iteration)
            status = iteration.run(folder_path,
                                   self.get_exp_config(),
                                   self.get_machine_config(),
                                   total_args.pprint,
                                   total_args.use_perf,
                                   print_stats)
            if not status:
                time.sleep(5)
                for i in range(utils.NUM_RETRIES):
                    utils.debug("Retrying iteration because it failed for the ",
                                "{}th time.".format(i+1))
                    status = iteration.run(folder_path,
                                   self.get_exp_config(),
                                   self.get_machine_config(),
                                   total_args.pprint,
                                   total_args.use_perf,
                                   print_stats)
                    if status:
                        break
            if not status:
                utils.warn("Failed to execute program after {} retries.".format(
                    utils.NUM_RETRIES))
                exit(1)
            
            ret = ""
            # append optional skip info about this trial to the skip
            self.append_to_skip_info(total_args, iteration, folder_path)
            if not total_args.pprint:
                time.sleep(2)
    def get_loop_yaml(self):
        return self.loop_yaml
    def execute(self, parser, namespace):
        total_args = self.add_specific_args(parser, namespace)
        if total_args.exp_type == "loop":
            if total_args.loop_config is None:
                utils.error("For experiment type loop, must provide loop config")
                exit(1)
            self.loop_yaml = yaml.load(Path(total_args.loop_config).read_text(),
                    Loader = yaml.FullLoader)
        else:
            self.loop_yaml = {}
        iterations = self.get_iterations(total_args)
        utils.debug("Number of iterations: {}".format(len(iterations)))
        # run the experiment (s) and analysis
        if total_args.exp_type == "loop":
            if not(total_args.analysis_only) and not(total_args.graph_only):
                self.run_iterations(total_args, iterations, print_stats=False)
            if not(total_args.no_analysis) and not(namespace.pprint) and not(total_args.graph_only):
                self.run_analysis_loop(total_args,
                                       iterations,
                                       print_stats=False,
                                       logfile=total_args.logfile)
            if not(total_args.no_graph) and not(namespace.pprint):
                self.run_graphing_scripts(total_args, total_args.folder,
                                          logfile=total_args.logfile)

        elif total_args.exp_type == "individual":
            if not(total_args.analysis_only):
                self.run_iterations(total_args, iterations, print_stats=True)
            if not(total_args.no_analysis) and not(namespace.pprint):
                self.run_analysis_loop(total_args,
                                       iterations,
                                       print_stats=True)


class Iteration(metaclass=abc.ABCMeta):
    @ abc.abstractmethod
    def __str__(self):
        return

    @ abc.abstractmethod
    def get_iteration_params(self):
        """
        Returns array of parameters this iteration could vary (for the csv
        file). For ordering in logfile csv.
        """
        return

    @ abc.abstractmethod
    def get_iteration_params_values(self):
        """
        Returns dictionary of iteration params.
        """
        return

    def get_iteration_avg_message_size(self):
        """
        Returns the average message size for the iteration
        """
        return

    def get_host_list(self, host_type_map):
        """
        Get list of all hosts involved in running this program, for client and
        server program
        """
        ret = []
        if "server" in host_type_map:
            ret.extend(host_type_map["server"])
        if "client" in host_type_map:
            ret.extend(self.get_iteration_clients(host_type_map["client"]))
        return ret
    
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
    
    def get_program_hosts(self, program_name, host_type_map):
        ret = []
        if program_name == "start_server":
            return host_type_map["server"]
        elif program_name == "start_client":
            return self.get_iteration_clients(host_type_map["client"]) 
    

    def get_program_version_info(self, host_cornflakes_dir, host, cxn):
        ret = {}
        res = cxn.run("git branch", 
                wd = host_cornflakes_dir,
                quiet = True)
        if res.exited == 0:
            ret["cornflakes_branch"] = res.stdout
        res = cxn.run("git rev-parse HEAD", 
                wd = host_cornflakes_dir, 
                quiet = True)
        if res.exited == 0:
            ret["cornflakes_hash"] = res.stdout
        res = cxn.run("git status --short", 
                wd = host_cornflakes_dir, 
                quiet = True)
        if res.exited == 0:
            ret["cornflakes_status"] = res.stdout
        res = cxn.run("git submodule status", 
                wd = host_cornflakes_dir, 
                quiet = True)
        if res.exited == 0:
            ret["cornflakes_submodule_status"] = res.stdout
        return ret

    def read_key_from_analysis_log(self, folder_path, key):
        df = self.read_analysis_log(folder_path)
        col = df[key]
        print(df[key])
        try:
            return col[0]
        except:
            raise ValueError("Could not read value of {} from df {}".format(key, df))
            
    # returns dataframe representing the data
    def read_analysis_log(self, local_folder):
        analysis_path = self.get_folder_name(local_folder) /\
                "analysis.log"
        return pd.read_csv(analysis_path)

    def calculate_iteration_stats(self, local_folder,
            client_file_list, print_stats):
        """
        Analyzes this iteration stats.
        If print_stats is true, prints out values of parameters and values
        of stats.
        Default implementation assumes Rust clients that implement the state
        machine trait.
        Otherwise, writes csv into local_folder/analysis.log.
        """
        # TODO: iterate over all "client_type" hosts used by this
        # iteration
        # collect and sum histogram
        # collect and sum achieved load pps
        # convert total achieved load pps to gbps
        packets_received = 0
        packets_sent = 0
        max_runtime = 0
        histogram = utils.Histogram({})
        for filename in client_file_list:
            # format: json file with map 1: histogram
            # map 2: map from int -> {thread_stats}
            yaml_map = yaml.load(Path(filename).read_text(),
                    Loader=yaml.FullLoader)
            histogram_map = yaml_map[0]
            client_histogram = utils.Histogram(histogram_map)
            # combine client histogram into total histogram
            histogram.combine(client_histogram)

            # count packets sent and received
            threads_map = yaml_map[1]
            for thread, thread_info in threads_map.items():
                packets_sent += thread_info["num_sent"]
                packets_received += thread_info["num_received"]
                thread_runtime = thread_info["runtime"]
                max_runtime = max(thread_runtime, max_runtime)

        # calculate full statistics
        achieved_load_pps = float(packets_received) / max_runtime
        achieved_load_pps_sent = float(packets_sent) / max_runtime
        achieved_load_gbps = utils.get_tput_gbps(achieved_load_pps,
                self.get_iteration_avg_message_size())
        achieved_load_gbps_sent = utils.get_tput_gbps(achieved_load_pps_sent, self.get_iteration_avg_message_size())

        iteration_params = self.get_iteration_params_values()
        # assumes iteration param has offered load pps
        offered_load_pps = iteration_params["offered_load_pps"]
        offered_load_gbps = iteration_params["offered_load_gbps"]
        percent_achieved = float(achieved_load_pps) / float(offered_load_pps)

        iteration_params["achieved_load_pps"] = achieved_load_pps
        iteration_params["achieved_load_pps_sent"] = achieved_load_pps_sent
        iteration_params["achieved_load_gbps"] = achieved_load_gbps
        iteration_params["achieved_load_gbps_sent"] = achieved_load_gbps_sent
        iteration_params["percent_achieved_rate"] = percent_achieved
        iteration_params["avg"] = histogram.avg() / float(1000)
        iteration_params["median"] = histogram.value_at_quantile(0.50) / float(1000)
        iteration_params["p99"] = histogram.value_at_quantile(0.99) / float(1000)
        iteration_params["p999"] = histogram.value_at_quantile(0.999) / float(1000)

        format_string_params = ["{{{}}}".format(x) for x in
                self.get_csv_header()]
        format_string = ",".join(format_string_params)
        format_string = format_string.format(**iteration_params)

        analysis_path = Path(local_folder) / "analysis.log"
        with open(str(analysis_path), "w") as f:
            f.write(",".join(self.get_csv_header()) + os.linesep)
            f.write(format_string + os.linesep)
            f.close()
        if print_stats:
            utils.info("Experiment results:\n"
                               "\t- offered load: {:.4f} req/s | {:.4f} Gbps\n"
                               "\t- achieved load: {:.4f} req/s | {:.4f} Gbps\n"
                               "\t- percentage achieved rate: {:.4f}\n"
                               "\t- avg latency: {:.4f}"
                               " \u03BCs\n\t- median: {:"
                               ".4f} \u03BCs\n\t- p99: {: .4f}"
                               "\u03BCs\n\t- p999:"
                               "{: .4f} \u03BCs".format(
                                   offered_load_pps, offered_load_gbps,
                                   achieved_load_pps, achieved_load_gbps,
                                   percent_achieved,
                                   iteration_params["avg"],
                                   iteration_params["median"],
                                   iteration_params["p99"],
                                   iteration_params["p999"]))
    def get_csv_header(self):
        csv_order = self.get_iteration_params()
        csv_order.extend(["achieved_load_pps", "achieved_load_pps_sent",
        "achieved_load_gbps", "achieved_load_gbps_sent",
        "percent_achieved_rate", "avg", "median", "p99", "p999"])
        return csv_order

    @ abc.abstractmethod
    def get_folder_name(self, high_level_folder):
        return

    @ abc.abstractmethod
    def get_program_args(self,
                         host,
                         program_name,
                         programs_metadata):
        """
        Given a program name specified in the exp_yaml, return the arguments
        corresponding to that program.
        """
        return

    def create_folder(self, high_level_folder):
        folder_name = self.get_folder_name(high_level_folder)
        folder_name.mkdir(parents=True, exist_ok=True)
    def delete_folder(self, high_level_folder):
        folder_name = self.get_folder_name(high_level_folder)
        shutil.rmtree(folder_name)

    def run(self, local_results, exp_config, machine_config, pprint,
            use_perf=False, print_stats = False):
        """
        Runs the actual program.
        Arguments:
            * local_results - Folder where local results go (not specific to
            this iteration).
            * exp_config - Experiment yaml that contains command lines. Assumes
            this contains a set of programs to run, each with a list of
            corresponding hosts that can run that command line.
            * machine_config - Machine level config yaml.
            * pprint - Instead of running, just print out command lines.
            * use_perf - Whether to use perf or not when running the server.
        """
        # generate a random seed for this experiment
        random_seed = int(time.time())
        programs = exp_config["programs"]
        exp_time = exp_config["time"]

        # recording the arguments used to run this invocation
        record_paths = {}

        # map from a program id to the actual process
        program_counter = 0
        # status of each (program_name, host)
        status_dict = {}
        # map of hosts -> connections
        connections = {}

        # map of (program_name, host) -> program_args
        program_args_map = {}

        # program commands
        program_cmds = {}

        # command queue for background processes
        server_command_queue = []
        client_command_queue = []

        # create a local folder path for results
        local_results_path = self.get_folder_name(local_results)
        self.create_folder(local_results)
        
        host_type_map = machine_config["host_types"]
        program_host_list = self.get_host_list(host_type_map)
        program_run_kwargs = {}

        # record paths: maps (program_name, host) to command run to record
        record_paths = {}
        
        # populate program host list
        for host in program_host_list:
            host_tmp = machine_config["hosts"][host]["tmp_folder"]
            host_addr = machine_config["hosts"][host]["addr"]
            key = machine_config["key"]
            user = machine_config["user"]               
            try:
                connection_wrapper = connection.ConnectionWrapper(
                                        addr = host_addr,
                                        user = user,
                                        port = 22,
                                        key = key)
                connections[host] = connection_wrapper
            except:
                utils.warn("Failed to ssh")
                time.sleep(60)
                return False
                
            # for this host, create the remote temporary folder
            remote_tmp_path = self.get_folder_name(machine_config["hosts"][host]["tmp_folder"])
            if not pprint:
                connections[host].mkdir(remote_tmp_path)
        
        # populate  program args
        for program_name in programs:
            program = programs[program_name]
            program_hosts = self.get_program_hosts(program_name,
                    host_type_map)
            for host in program_hosts:
                # populate program args
                # NOTE: random seed should be the *same* across all client
                # hosts
                program_args = self.get_program_args(host,
                                                     machine_config,
                                                     program_name,
                                                     programs)
                program_args["cornflakes_dir"] = machine_config["hosts"][host]["cornflakes_dir"]
                program_args["config_file"] = machine_config["hosts"][host]["config_file"]
                program_args["folder"] = self.get_folder_name(host_tmp)
                program_args["local_folder"] = local_results_path
                program_args["host"] = host
                program_args["time"] = exp_time
                program_args["random_seed"] = random_seed
                program_args_map[(program_name, host)] = program_args

                program_cmd = program["start"].format(**program_args)
                stdout = None
                stderr = None
                if "log" in program:
                    if "out" in program["log"]:
                        stdout = program["log"]["out"].format(**program_args)
                    if "err" in program["log"]:
                        stderr = program["log"]["err"].format(**program_args)
                if use_perf and "perf" in program:
                    perf_cmd = program["perf"].format(**program_args)
                    program_cmd = "{} {}".format(perf_cmd, program_cmd)
                if pprint:
                    utils.debug("Host = {}, Running: {}".format(host, program_cmd))
                else:
                    program_host_type = program["host_type"]
                    background_arg = False

                    if program_host_type == "server":
                        server_command_queue.append((program_name, host))
                        background_arg = True
                    elif program_host_type == "client":
                        client_command_queue.append((program_name, host))
                    program_cmds[(program_name, host)] = program_cmd
                    record_paths[(program_name, host)] = {"host": host, "args": program_args, "command": program_cmd}
                    program_run_kwargs[(program_name, host)] = {
                            "cmd": program_cmd,
                            "stdin": None,
                            "stdout": stdout,
                            "stderr": stderr,
                            "ignore_out": False,
                            "wd": None,
                            "sudo": True,
                            "background": background_arg,
                            "quiet": False,
                            "pty": True,
                            "res_map": status_dict,
                            "res_key": (program_name, host)
                            }
        def client_has_ready_file():
            for program_name, host in client_command_queue:
                program = programs[program_name]
                if "ready_file" in program:
                    return True
            return False
        # function to check whether a certain program can be started
        def is_ready(other_program_name):
            other_program = programs[other_program_name]
            program_host_list = self.get_program_hosts(other_program_name, host_type_map)
            for other_host in program_host_list:
                other_program_args = program_args_map[(other_program_name,
                        other_host)]
                for (ready_file, ready_string) in other_program["ready_file"].items():
                    ready_file = ready_file.format(**other_program_args)
                    if not(connections[other_host].check_ready(ready_file, ready_string)):
                        return False
            return True

        # start server programs
        ct = 0
        server_failed = False
        for program_name, host in server_command_queue:
            program = programs[program_name]
            connections[host].run(**program_run_kwargs[(program_name,
                host)])
            program_args = program_args_map[(program_name, host)]
            binary_name = program["binary_name"].format(**program_args_map[(program_name, host)])
            # give some time for process to run or fail
            time.sleep(1)
            while not(connections[host].check_proc(binary_name)):
                res = status_dict[(program_name, host)]
                stdout = res.stdout
                stderr = res.stderr
                if "out" in program["log"]:
                    stdout = connections[host].read_file(program["log"]["out"].format(**program_args))
                if "err" in program["log"]:
                    stderr = connections[host].read_file(program["log"]["err"].format(**program_args))
                utils.warn("Program {} on host {} failed:\n\t- stdout: "\
                            "{}\n\t- stderr: "\
                    "{}".format(program_name, host, stdout,
                        stderr))
                # kill all processes until this
                for i in range(0, ct):
                    prev_program = server_command_queue[i][0]
                    prev_host = server_command_queue[i][1]
                    res = connections[prev_host].stop_background_binary(
                        programs[prev_program]["binary_to_stop"].format(**program_args_map[(prev_program,
                            prev_host)]),
                            quiet = True,
                            sudo = True)
                server_failed = True
                break
            if server_failed:
                break
            if not(client_has_ready_file()): 
               # if no client wait for ready, wait for server
                while not(is_ready(program_name)):
                    time.sleep(1)
                    continue
            ct += 1
        
        # wait for input
        if not(server_failed):
            clients = [threading.Thread(
            target = run_client,
            kwargs = {"cxn": connections[host], "kwargs":
                program_run_kwargs[(program_name, host)]})
            for program_name, host in client_command_queue]
            [c.start() for c in clients]

        ## if client has ready, wait on servers to be done
        ## and write client ready files
            if (client_has_ready_file()):
                utils.info("Spawned clients, waiting for server ready")
                for program_name, host in server_command_queue:
                    while not(is_ready(program_name)):
                        time.sleep(1)
                        continue
                for program_name, host in client_command_queue:
                    client_program = programs[program_name]
                    client_program_args = program_args_map[(program_name, host)]
                    for (ready_file, ready_string) in client_program["ready_file"].items():
                        ready_file = ready_file.format(**client_program_args)
                        connections[host].write_ready(ready_file, ready_string)
                    utils.info("Writing ready for client host {}".format(host))
            [c.join() for c in clients]

        ## kill the server
        if not(server_failed):
            for program_name, host in server_command_queue:
                program = programs[program_name]
                program_args = program_args_map[(program_name, host)]
                binary_to_stop = program["binary_to_stop"].format(**program_args)
                res = connections[host].stop_background_binary(
                    binary_to_stop,
                    quiet = True,
                    sudo = True)
                if res.exited != 0:
                    utils.warn("Failed to kill server: stdout: {} stderr: {}".format(res.stdout, res.stderr))
                if "extra_stop" in program:
                    res = connections[host].stop_with_pkill(
                            program["binary_name"].format(**program_args),
                            quiet = True,
                            sudo = True)
                    if res.exited != 0:
                        utils.warn("Failed to kill server: stdout: {} stderr: {}".format(res.stdout, res.stderr))


        any_failed = server_failed
        if not(server_failed):
            for program_name, host in client_command_queue:
                program = programs[program_name]
                program_args = program_args_map[(program_name, host)]
                status = status_dict[(program_name, host)]
                stdout = status.stdout
                stderr = status.stderr
                if "out" in program["log"]:
                    stdout = connections[host].read_file(program["log"]["out"].format(**program_args))
                if "err" in program["log"]:
                    stderr = connections[host].read_file(program["log"]["err"].format(**program_args))
                utils.warn("Program {} on host {} failed:\n\t- stdout: "\
                            "{}\n\t- stderr: "\
                    "{}".format(program_name, host, stdout,
                        stderr))
                if status.exited != 0:
                    utils.warn("Program {} on host {} failed; stdout: {}; stderr:"\
                    "{}".format(program_name, 
                        host, 
                        stdout,
                        stderr))
                    any_failed = True

        if not(any_failed):
            if not pprint:
                for program_name in programs:
                    program = programs[program_name]
                    program_hosts = self.get_program_hosts(program_name, host_type_map)
                    # transfer all logs locally
                    for host in program_hosts:
                        program_args = program_args_map[(program_name, host)]
                        program_args_copy = copy.deepcopy(program_args)
                        for filetype, filename in program["log"].items():
                            remote_file = filename.format(**program_args)
                            program_args_copy["folder"] = local_results_path
                            local_file = filename.format(**program_args_copy)
                            connections[host].get(remote_file, local_file)

        # delete all files, even if stuff has failed
        for host in program_host_list:
            # delete all remote tmp files
            host_tmp = machine_config["hosts"][host]["tmp_folder"]
            connections[host].run("rm -rf {}".format(host_tmp), 
                    sudo = True,
                    quiet = True)

        # if any failed, delete local results path
        if pprint:
            return True
        if any_failed:
            self.delete_folder(local_results)
            return False
        
        program_version_info = {}
        for host in program_host_list:
            program_version_info[host] = self.get_program_version_info(
                    machine_config["hosts"][host]["cornflakes_dir"],
                    host, 
                    connections[host])
            record_paths["program_version_info"] =  program_version_info
        local_record = str(Path(local_results_path) / "record.log")
        with open(local_record, 'w') as file:
            yaml.dump(record_paths, file)
            file.close()
            
        client_list = self.get_program_hosts("start_client",
                    host_type_map)
        client_file_list = []
        for host in client_list:
            program_args = program_args_map[("start_client", host)]
            program_args_copy = copy.deepcopy(program_args)
            program_args_copy["folder"] = local_results_path
            client_file_list.append(programs["start_client"]["log"]["results"].format(**program_args_copy))
        # run analysis
        if not pprint:
            self.calculate_iteration_stats(
                    local_results_path, client_file_list, print_stats)
        return True

    def get_program_args_map(self, 
            exp_config, 
            host_type_map,
            machine_config,
            local_results):
        programs = exp_config["programs"]
        program_args_map = {}
        for program_name in programs:
            program = programs[program_name]
            program_hosts = self.get_program_hosts(program_name,
                    host_type_map)
            for host in program_hosts:
                # populate program args
                host_tmp = machine_config["hosts"][host]["tmp_folder"]
                local_results_path = self.get_folder_name(local_results)
                program_args = self.get_program_args(host,
                                                     machine_config,
                                                     program_name,
                                                     programs)
                program_args["cornflakes_dir"] = machine_config["hosts"][host]["cornflakes_dir"]
                program_args["config_file"] = machine_config["hosts"][host]["config_file"]
                program_args["folder"] = self.get_folder_name(host_tmp)
                program_args["local_folder"] = local_results_path
                program_args["host"] = host
                program_args["time"] = exp_config["time"]
                program_args_map[(program_name, host)] = program_args
        return program_args_map

    def get_client_file_list(self, exp_config, program_args_map, host_type_map,
            local_results):
        programs = exp_config["programs"]
        client_list = self.get_program_hosts("start_client",
                    host_type_map)
        client_file_list = []
        for host in client_list:
            local_results_path = self.get_folder_name(local_results)
            program_args = program_args_map[("start_client", host)]
            program_args_copy = copy.deepcopy(program_args)
            program_args_copy["folder"] = local_results_path
            client_file_list.append(programs["start_client"]["log"]["results"].format(**program_args_copy))
        return client_file_list
        

def run_client(cxn, kwargs):
    cxn.run(**kwargs)



