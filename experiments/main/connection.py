#!/usr/bin/python3

from fabric import Connection
import agenda
import argparse
import os
import shutil
import subprocess
import sys
import threading
import time
import toml
from main import utils

class ConnectionWrapper(Connection):
    def __init__(self, addr, user=None, port=22, key=None):
        connect_kwargs = {}
        connect_kwargs["gss_auth"] = True
        connect_kwargs["gss_deleg_creds"] = True
        connect_kwargs["gss_kex"] = True
        utils.debug(addr, user, port, key)
        if key is not None:
            connect_kwargs["key_filename"] = [key]
            connect_kwargs["banner_timeout"] = 200
        super().__init__(
            host = addr,
            user = user,
            port = port,
            forward_agent = True,
            connect_kwargs = connect_kwargs,
        )
        self.addr = addr
        self.conn_addr = addr

        # Start the ssh connection
        super().open()

    """
    Run a command on the remote machine
    verbose    : if true, print the command before running it, and any output it produces
                 (if not redirected)
                 if false, capture anything produced in stdout and save in result (res.stdout)
    background : if true, start the process in the background via nohup.
                 if output is not directed to a file or pty=True, this won't work
    stdin      : string of filename for stdin (default /dev/stdin as expected)
    stdout     : ""
    stderr     : ""
    ignore_out : shortcut to set stdout and stderr to /dev/null
    wd         : cd into this directory before running the given command
    sudo       : if true, execute this command with sudo (done AFTER changing to wd)
    returns result struct
        .exited = return code
        .stdout = stdout string (if not redirected to a file)
        .stderr = stderr string (if not redirected to a file)
    """
    def run(self, cmd, *args, stdin=None, stdout=None, stderr=None,
            ignore_out=False, wd=None, sudo=False, background=False,
            quiet=False, pty=True, res_map = {}, res_key = None, **kwargs):
        self.verbose = True
        # Prepare command string
        pre = ""
        if wd:
            pre += f"cd {wd} && "
        if background:
            pre += "screen -d -m "
        #escape the strings
        cmd = cmd.replace("\"", "\\\"")
        if sudo:
            pre += "sudo "
        pre += "bash -c \""
        if ignore_out:
            stdin="/dev/null"
            stdout="/dev/null"
            stderr="/dev/null"
        if background:
            stdin="/dev/null"

        full_cmd = f"{pre}{cmd}"
        if stdout is not None:
            full_cmd  += f" > {stdout} "
        if stderr is not None:
            full_cmd  += f" 2> {stderr} "
        if stdin is not None:
            full_cmd  += f" < {stdin} "

        full_cmd += "\""

        # Prepare arguments for invoke/fabric
        if background:
            pty=False

        # Print command if necessary
        if not quiet:
            agenda.subtask("[{}]{} {}".format(self.addr.ljust(10), " (bg) " if background else "      ", full_cmd))

        # Finally actually run it
        res = super().run(full_cmd, *args, hide=True, warn=True, pty=pty, **kwargs)
        if res_key == ('start_client', 'client1'):
            utils.debug("Finished running start client command")
        
        if res_key is not None:
            res_map[res_key] = res

        return res

    def stop_background_binary(self, binary_name, quiet = False, sudo = False):
        stop_command = f"kill -9 `ps aux | grep {binary_name} | grep SCREEN | grep -v grep | awk '{{print $2}}'`"
        return self.run(stop_command, quiet = quiet, sudo = sudo)
    
    def stop_with_pkill(self, binary_name, quiet = False, sudo = False):
        stop_command = f"pkill -f -9 {binary_name}"
        return self.run(stop_command, quiet = quiet, sudo = sudo)

    def file_exists(self, fname):
        res = self.run(f"ls {fname}", quiet = True)
        return res.exited == 0

    def check_ready(self, fname, fstring):
        if not(self.file_exists(fname)):
            return False
        res = self.run(f"cat {fname}", quiet = True)
        if res.exited != 0:
            return False
        else:
            return str(res.stdout).strip() == fstring

    def prog_exists(self, prog):
        res = self.run(f"which {prog}")
        return res.exited == 0

    def read_file(self, fname):
        res = self.run(f"cat {fname}", quiet = True)
        if res.exited == 0:
            return str(res.stdout)
        return None


    def check_proc(self, proc_name):
        res = self.run(f"pgrep -f {proc_name}", quiet = True)
        if res.exited != 0:
            agenda.subfailure(f'failed to find running process with name \"{proc_name}\" on {self.addr}')
            return False
        return True


    def check_file(self, grep, where):
        res = self.run(f"grep \"{grep}\" {where}")
        if res.exited != 0:
            agenda.subfailure(f"Unable to find search string (\"{grep}\") in process output file {where}")
            res = self.run(f'tail {where}')
            if res.exited == 0:
                print(res.command)
                print(res.stdout)
            sys.exit(1)

    def local_path(self, path):
        r = self.run(f"ls {path}")
        return r.stdout.strip().replace("'", "")

    def put(self, local_file, remote=None, preserve_mode=True):
        if remote and remote[0] == "~":
            remote = remote[2:]
        agenda.subtask("[{}] scp localhost:{} -> {}:{}".format(
            self.addr,
            local_file,
            self.addr,
            remote
        ))

        return super().put(local_file, remote, preserve_mode)

    def mkdir(self, remote_path):
        self.run(f"mkdir -p {remote_path}", quiet = True)

    def get(self, remote_file, local=None, preserve_mode=True):
        if local is None:
            local = remote_file

        agenda.subtask("[{}] scp {}:{} -> localhost:{}".format(
            self.addr,
            self.addr,
            remote_file,
            local
        ))

        return super().get(remote_file, local=local, preserve_mode=preserve_mode)

def get_local(filename, local=None, preserve_mode=True):
    assert(local is not None)
    subprocess.run(f"mv {filename} {local}", shell=True)
