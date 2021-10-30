import abc
import yaml
import argparse
import os
from main import utils
from pathlib import Path
import multiprocessing as mp
import subprocess as sh
from fabric import Connection
import git
from collections import defaultdict
import time
from tqdm import tqdm


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
    def get_git_directories(self):
        """
        Returns list of main git directories the program is stored in.
        Used to store hashes of programs in experiment log.
        """
        return

    def get_program_version_info(self):
        ret = {}
        for git_dir in self.get_git_directories():
            repo = git.Repo(git_dir)
            branch = repo.active_branch.name
            sha = repo.head.object.hexsha
            is_dirty = repo.is_dirty()
            ret[git_dir] = {"branch": branch, "hash": sha, "is_dirty":
                            is_dirty}
        return ret

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
    def run_analysis_individual_trial(self,
                                      higher_level_folder,
                                      program_metadata,
                                      iteration,
                                      print_stats=False):
        """
        Runs individual analysis for the trial just happened.
        """
        return

    @abc.abstractmethod
    def graph_results(self, total_args, folder, logfile):
        return

    def run_graphing_scripts(self, total_args, folder, logfile=None):
        if logfile is None:
            return
        self.graph_results(total_args, folder, logfile)

    def run_analysis_loop(self, total_args, iterations, print_stats=False, logfile=None):
        """
        Runs analysis in a loop for each iteration.
        Includes running any graphing.
        """
        pool = mp.Pool(mp.cpu_count())
        pool_args = []
        folder_path = Path(total_args.folder)
        program_metadata = self.get_exp_config()["programs"]
        f = None
        if logfile is not None:
            logfile_path = folder_path / logfile
            f = open(logfile_path, "w")
            f.write(self.get_logfile_header() + os.linesep)

        ct = 0
        for iteration in iterations:
            # use torch (Which should parallelize within iterations)
            utils.debug("Ct: {}".format(ct))
            ct += 1
            # check if the folder has "analysis.log"
            analysis_path = iteration.get_folder_name(folder_path) /\
                "analysis.log"
            ret = ""
            if (os.path.exists(analysis_path)):
                try:
                    with open(analysis_path) as analysis_file:
                        lines = analysis_file.readlines()
                        lines = [line.strip() for line in lines]
                        ret = lines[0]
                except:
                    # otherwise, try to parse again
                    ret = ""
            if (ret == ""):
                ret = self.run_analysis_individual_trial(folder_path,
                                                         program_metadata,
                                                         iteration, print_stats)
            if ret != "":
                if f is not None:
                    f.write(ret + os.linesep)
        if f is not None:
            f.close()

    def run_iterations(self, total_args, iterations, print_stats=False):
        """
        Loops over the iterations and runs each experiment.
        """
        program_version_info = self.get_program_version_info()
        folder_path = Path(total_args.folder)
        program_metadata = self.get_exp_config()["programs"]
        ct = 0
        total = len(iterations)
        start = time.time()
        expected_time = 20 * total / 3600.0
        utils.warn("Expected time to finish: {} hours".format(expected_time))
        for iteration in iterations:
            ct += 1
            if (ct > 1):
                rate_so_far = (ct - 1)/(time.time() - start)
                left = (total - (ct - 1))
                expected_time_to_finish = (
                    float(left) / (float(rate_so_far))) / 3600.0
                utils.info("Running iteration  # {} out of {}, {} % done with iterations expected time to finish: {} hours".format(
                    ct - 1, total, (float(ct - 1)/float(total) * 100.0),
                    expected_time_to_finish))
            if iteration.get_folder_name(folder_path).exists():
                utils.info("Iteration already exists, skipping:"
                           "{}".format(iteration))
                continue
            if not total_args.pprint:
                iteration.create_folder(folder_path)
            utils.debug("Running iteration: ", iteration)
            status = iteration.run(iteration.get_folder_name(folder_path),
                                   self.get_exp_config(),
                                   self.get_machine_config(),
                                   total_args.pprint,
                                   program_version_info,
                                   total_args.use_perf)
            if not status:
                time.sleep(5)
                for i in range(utils.NUM_RETRIES):
                    utils.debug("Retrying iteration because it failed for the ",
                                "{}th time.".format(i+1))
                    status = iteration.run(iteration.get_folder_name(folder_path),
                                           self.get_exp_config(),
                                           self.get_machine_config(),
                                           total_args.pprint,
                                           program_version_info)
                    if status:
                        break
            if not status:
                utils.warn("Failed to execute program after {} retries.".format(
                    utils.NUM_RETRIES))
                exit(1)
            # before next trial, run analysis
            ret = self.run_analysis_individual_trial(folder_path,
                                                     program_metadata,
                                                     iteration, print_stats)
            if ret != "":
                # open a file called "analysis.log" in the iteration folder and
                # write the folder there
                analysis_path = iteration.get_folder_name(folder_path) /\
                    "analysis.log"
                f = open(analysis_path, "w")
                if f is not None:
                    f.write(ret + os.linesep)
                    f.close()
            # because we've tried to do analysis, ok to sleep for less
            time.sleep(2)

    def execute(self, parser, namespace):
        total_args = self.add_specific_args(parser, namespace)
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
    def get_folder_name(self, high_level_folder):
        return

    @ abc.abstractmethod
    def get_hosts(self, program_name, programs_metadata):
        return

    @ abc.abstractmethod
    def get_program_args(self,
                         folder,
                         program_name,
                         host,
                         config_yaml,
                         programs_metadata,
                         exp_time):
        """
        Given a program name specified in the exp_yaml, return the arguments
        corresponding to that program.
        """
        return

    @ abc.abstractmethod
    def get_relevant_hosts(self, program):
        """
        Returns the relevant hosts for this program in the yaml.
        """
        return

    def create_folder(self, high_level_folder):
        folder_name = self.get_folder_name(high_level_folder)
        folder_name.mkdir(parents=True, exist_ok=True)

    def new_connection(self, host, machine_config):
        host_addr = machine_config["hosts"][host]["addr"]
        key = machine_config["key"]
        user = machine_config["user"]
        cxn = Connection(host=host_addr,
                         user=user,
                         port=22,
                         connect_kwargs={"key_filename": key})
        return cxn

    def kill_remote_process(self, cmd, host, machine_config):
        key = machine_config["key"]
        user = machine_config["user"]
        host_addr = machine_config["hosts"][host]["addr"]

        ssh_command = "ssh -i {} {}@{} {}".format(key, user, host_addr, cmd)
        sh.run(ssh_command, timeout=10, shell=True)

    def run_cmd_sudo(self, cmd, host, machine_config, fail_ok=False, return_dict=None, proc_counter=None):

        cxn = self.new_connection(host, machine_config)
        res = None
        try:
            res = cxn.sudo(cmd, hide=False)
            # utils.warn("Ran command: {}".format(cmd))
            # res.stdout.strip()
            if return_dict is not None and proc_counter is not None:
                return_dict[proc_counter] = True
            return
        except:
            if not fail_ok:
                utils.error(
                    "Failed to run cmd {} on host {}.".format(cmd, host))
                if return_dict is not None and proc_counter is not None:
                    return_dict[proc_counter] = False
                return
            if return_dict is not None and proc_counter is not None:
                return_dict[proc_counter] = True
                return

    def run(self, folder, exp_config, machine_config, pprint,
            program_version_info, use_perf=False):
        """
        Runs the actual program.
        Arguments:
            * folder - Path that all logfiles from this iteration should go.
            * exp_config - Experiment yaml that contains command lines. Assumes
            this contains a set of programs to run, each with a list of
            corresponding hosts that can run that command line.
            * machine_config - Machine level config yaml.
            * pprint - Instead of running, just print out command lines.
            * program_version_info - Metadata about the commit version of the
            repo at time of experiment.
            * use_perf - Whether to use perf or not when running the server.
        """
        programs_to_join_immediately = {}
        programs_to_kill = {}
        # map from start time (in seconds) to list
        # of programs with that start time
        programs_by_start_time = defaultdict(list)

        # assumes program processes to be executed are in order in the yaml
        commands = exp_config["commands"]
        programs = exp_config["programs"]
        exp_time = exp_config["time"]

        record_paths = {}

        # map from a program id to the actual process
        program_counter = 0
        proc_map = {}
        status_dict = {}
        manager = mp.Manager()
        status_dict = manager.dict()
        # spawn the commands
        for command in commands:
            program_name = command["program"]
            program = programs[program_name]
            program_hosts = program["hosts"]
            kill_cmd = None
            if "stop" in program:
                kill_cmd = program["stop"]
            for host in self.get_relevant_hosts(program, program_name):
                program_cmd = program["start"]
                if "log" in program:
                    if "out" in program["log"]:
                        stdout = program["log"]["out"]
                        program_cmd += " > {}".format(stdout)
                    if "err" in program["log"]:
                        stderr = program["log"]["err"]
                        program_cmd += " 2> {}".format(stderr)
                    if "record" in program["log"]:
                        record_path = program["log"]["record"]

                program_args = self.get_program_args(folder,
                                                     program_name,
                                                     host,
                                                     machine_config,
                                                     programs,
                                                     exp_time)
                program_cmd = program_cmd.format(**program_args)
                if use_perf and "perf" in program:
                    utils.debug("current program args: {}", program_args)
                    perf_cmd = program["perf"].format(**program_args)
                    program_cmd = "{} {}".format(perf_cmd, program_cmd)
                record_path = record_path.format(**program_args)
                fail_ok = False
                if kill_cmd is not None:
                    kill_cmd = kill_cmd.format(**program_args)
                    fail_ok = True

                yaml_record = {"host": host, "args": program_args, "command":
                               program_cmd,
                               "stop_command": kill_cmd, "version_info":
                               program_version_info}

                if pprint:
                    utils.debug(
                        "Host {}: \n\t - Running Cmd: {}\n\t - Stopped by: {}".format(host, program_cmd, kill_cmd))

                else:
                    record_paths[record_path] = yaml_record
                    proc = mp.Process(target=self.run_cmd_sudo,
                                      args=(program_cmd, host, machine_config,
                                            fail_ok, status_dict,
                                            program_counter))

                    start_time = int(command["begin"])
                    proc_map[program_counter] = proc
                    programs_by_start_time[start_time].append((kill_cmd,
                                                               program_counter,
                                                               program_name,
                                                               host,
                                                               program_args))
                    program_counter += 1
        # now start each start program
        cur_time = 0
        program_start_times = sorted(programs_by_start_time.keys())
        for start_time in program_start_times:
            if start_time != cur_time:
                time.sleep(start_time - cur_time)
            cur_time = start_time
            progs = programs_by_start_time[start_time]
            for info in progs:
                kill_cmd = info[0]
                program_counter = info[1]
                program_name = info[2]
                host = info[3]
                proc = proc_map[program_counter]
                program_args = info[4]
                utils.debug("Starting program {} on host {}, args: {}".format(
                    program_name, host, program_args))
                proc.start()
                if kill_cmd == None:
                    programs_to_join_immediately[host] = program_counter
                else:
                    programs_to_kill[host] = (program_counter,
                                              kill_cmd)
                #input("Press Enter to continue...")

        any_failed = False
        # now join all of the joining programs
        for host in programs_to_join_immediately:
            prog_counter = programs_to_join_immediately[host]
            proc = proc_map[prog_counter]
            res = proc.join()
            status = status_dict[prog_counter]
            if not status:
                any_failed = True
            utils.debug("Host {} done; status: {}".format(host, status))

        # now kill the rest of the programs
        for host in programs_to_kill:
            (program_counter, kill_cmd) = programs_to_kill[host]
            try:
                kill_cmd_with_sleep = kill_cmd + "; /bin/sleep 3"
                utils.debug(
                    "Trying to run kill command: {} on host {}".format(kill_cmd, host))
                self.kill_remote_process(kill_cmd_with_sleep, host,
                                         machine_config)
            except:
                utils.warn("Failed to run kill command:",
                           "{}".format(kill_cmd_with_sleep))
                exit(1)
            try:
                proc_map[program_counter].join()
            except:
                utils.warn(
                    "Failed to run join command: {}".format(program_counter))

        # now, experiment is over, so record experiment metadata
        for record_path in record_paths:
            yaml_record = record_paths[record_path]
            with open(record_path, 'w') as file:
                yaml.dump(yaml_record, file)
            file.close()

        if any_failed:
            utils.error("One of the programs failed.")
            return False
        return True
