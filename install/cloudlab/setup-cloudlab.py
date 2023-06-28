#### This script sets up Cornflakes on Cloudlab machines
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
import yaml as pyyaml

SSH_CORNFLAKES= "git@github.com:deeptir18/cornflakes.git"
HTTPS_CORNFLAKES = "https://github.com/deeptir18/cornflakes.git"

class ConnectionWrapper(Connection):
    def __init__(self, addr, user=None, port=22, key=None):
        connect_kwargs = {}
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

    def get_addr(self):
        return self.addr
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
    def run(self, cmd, *args, stdin=None, stdout=None, stderr=None, ignore_out=False, wd=None, sudo=False, background=False, quiet=False, pty=True, **kwargs):
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
        return super().run(full_cmd, *args, hide=True, warn=True, pty=pty, **kwargs)

    def file_exists(self, fname):
        res = self.run(f"ls {fname}")
        return res.exited == 0

    def prog_exists(self, prog):
        res = self.run(f"which {prog}")
        return res.exited == 0

    def check_proc(self, proc_name, proc_out):
        res = self.run(f"pgrep {proc_name}")
        if res.exited != 0:
            agenda.subfailure(f'failed to find running process with name \"{proc_name}\" on {self.addr}')
            res = self.run(f'tail {proc_out}')
            if res.exited == 0:
                print(res.command)
                print(res.stdout)
            else:
                print(res)
            sys.exit(1)


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


def check(ok, msg, addr, allowed=[]):
    # exit code 0 is always ok, allowed is in addition
    if ok.exited != 0 and ok.exited not in allowed:
        agenda.subfailure(f"{msg} on {addr}: {ok.exited} not in {allowed}")
        agenda.subfailure("stdout")
        print(ok.stdout)
        agenda.subfailure("stderr")
        print(ok.stderr)
        global thread_ok
        thread_ok = False # something went wrong.
        raise Exception(f"{msg} on {addr}: {ok.exited} not in {allowed}")

def main():
    # define argparser
    parser = argparse.ArgumentParser(prog='SETUP CLOUDLAB')
    parser.add_argument('--server', 
                        required = True,
                        help = "IP to ssh into"\
            "server machine in cluster")
    parser.add_argument('--clients', 
                        nargs = "+", 
                        required = True, 
                        help = "IPs to ssh into client machines in cluster")
    parser.add_argument('--cloudlab_key', 
                        help = "Path to ssh key on this machine.",
                        required = True) 
    parser.add_argument('--iface_name', 
                        help = "Interface name for research interface on"\
                        "machines. For c6525-100g machines, this is "\
                        "`ens1f0`",
                        required = True) 
    parser.add_argument('--user', 
                        help = "username",
                        required = True) 
    parser.add_argument('--port', 
                        help = "SSH port",
                        type = int,
                        default = 22) 
    parser.add_argument('--mount_file', 
                        help = "Path to mount file",
                        default="/mydata") 
    parser.add_argument('--clone_with_ssh',
                        action='store_true',
                        help = "Clones cornflakes into machines with ssh")
    parser.add_argument('--setup_mount',
                        action='store_true',
                        help = "Runs commands for basic setup.")
    parser.add_argument('--clone_cornflakes',
                        action='store_true',
                        help = "Runs commands to clone cornflakes")
    parser.add_argument('--install_deps',
                        action='store_true',
                        help = "Runs commands for installing dependencies.")
    parser.add_argument('--generate_config',
                        action='store_true',
                        help = "Generate config and store on machines.")
    parser.add_argument('--mlx5',
                        action='store_true',
                        help = "Installs mellanox drivers.")
    parser.add_argument('--reset-network',
                        action='store_true',
                        help = "Runs commands to reset network.")
    parser.add_argument('--reboot-state',
                        action='store_true',
                        help = "Sets up huge pages and cstates.")
    parser.add_argument('--outfile', '-of',
                        help = "Write config file locally.")
    parser.add_argument('--control_machine', 
                                help = "name of control machine")
    parser.add_argument('--control_key_path', 
                                help = "ssh key path on control machine")
    args = parser.parse_args()
    # set up mount, clone cornflakes, download software on each machine
    machine_conns = {}
    machine_conns["server"] = setup_conn(args, "server", args.server)
    for i in range(1, len(args.clients) + 1):
        machine_conns[f"client{i}"] = setup_conn(args, f"client{i}", args.clients[i-1])

    if args.setup_mount:
        setup_mount(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            setup_mount(args, f"client{i}", args.clients[i-1], machine_conns)

    if args.clone_cornflakes:
        clone_cornflakes(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            clone_cornflakes(args, f"client{i}", args.clients[i-1], machine_conns)

    if args.install_deps:
        install_deps(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            install_deps(args, f"client{i}", args.clients[i-1], machine_conns)
   
    # query network configuration & create configuration yaml
    if args.generate_config:
        create_and_copy_machine_config(args, machine_conns)

    # run mellanox install (final)
    if args.mlx5:
        run_mellanox_install(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            run_mellanox_install(args, f"client{i}", args.clients[i-1], machine_conns)


    if args.reset_network:
        reset_network(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            reset_network(args, f"client{i}", args.clients[i-1], machine_conns)

    # setup huge pages
    if args.reboot_state:
        setup_huge_pages_and_cstates(args, "server", args.server, machine_conns)
        for i in range(1, len(args.clients) + 1):
            setup_huge_pages_and_cstates(args, f"client{i}", args.clients[i-1], machine_conns)

def setup_conn(args, machine_name, machine_ip):
    agenda.task(f"[{machine_name}: {machine_ip}] setup ssh connection")
    conn = ConnectionWrapper(machine_ip, 
                                args.user, 
                                args.port,
                                args.cloudlab_key)
    return conn

def setup_mount(args, machine_name, machine_ip, machine_conns):
    agenda.task(f"[{machine_name}: {machine_ip}] setup mount")
    conn = machine_conns[machine_name]
    # create folder with mount
    agenda.subtask(f"[{machine_name}: {machine_ip}] creating "\
    f"{args.mount_file}/{args.user}")
    conn.run(f"mkdir -p {args.mount_file}", quiet = True, sudo = True)
    conn.run(f"/usr/local/etc/emulab/mkextrafs.pl {args.mount_file}",
            quiet = True, sudo = True)
    conn.run(f"mkdir -p {args.mount_file}/{args.user}", quiet = True, sudo = True)
    conn.run(f"chown {args.user} {args.mount_file}/{args.user}", quiet =
            True, sudo = True)
    conn.run(f"mkdir {args.mount_file}/{args.user}/packages", quiet = True)
    conn.run(f"mkdir {args.mount_file}/{args.user}/config", quiet = True)

def clone_cornflakes(args, machine_name, machine_ip, machine_conns):
    agenda.task(f"[{machine_name}: {machine_ip}] clone cornflakes")
    conn = machine_conns[machine_name]
    user_wd = f"{args.mount_file}/{args.user}"
    cornflakes_dir = f"{args.mount_file}/{args.user}/cornflakes"
    packages_dir = f"{args.mount_file}/{args.user}/packages"
    
    if conn.file_exists(cornflakes_dir):
        conn.run(f"rm -rf {cornflakes_dir}")
    if args.clone_with_ssh:
        # authenticate with git first
        conn.run(f"git -T git@github.com -o StrictHostKeyChecking=no", quiet
                = False)
        conn.run(f"git clone {SSH_CORNFLAKES}", 
                wd = user_wd, 
                quiet = False)
    else:
        conn.run(f"git clone {HTTPS_CORNFLAKES}",
                wd = user_wd, 
                quiet = False)
    conn.run(f"git submodule update --init --recursive",
                wd = cornflakes_wd, 
                quiet = False)




def install_deps(args, machine_name, machine_ip, machine_conns):
    agenda.task(f"[{machine_name}: {machine_ip}] setup dependencies")
    
    user_wd = f"{args.mount_file}/{args.user}"
    cornflakes_dir = f"{args.mount_file}/{args.user}/cornflakes"
    packages_dir = f"{args.mount_file}/{args.user}/packages"
    conn = machine_conns[machine_name]

    # install Rust
    agenda.subtask(f"[{machine_name}: {machine_ip}] installing rust")
    conn.run("curl https://sh.rustup.rs -sSf | sh -s -- -y")

    agenda.subtask(f"[{machine_name}: {machine_ip}] setting up cornflakes"\
    " dependencies")
    # cornflakes apt-get and python dependencies
    conn.run(f"{args.mount_file}/{args.user}/cornflakes/install/install-dependencies.sh", wd = cornflakes_dir,
            quiet = False)
    # cornflakes package dependencies
    conn.run(f"PRIMARY=y {args.mount_file}/{args.user}/cornflakes/install/install-libraries.sh {packages_dir}", wd = cornflakes_dir)

    # install R
    conn.run(f"{args.mount_file}/{args.user}/cornflakes/install/install-R.sh {packages_dir}", wd = cornflakes_dir)

def create_and_copy_machine_config(args, machine_conns):
    agenda.task("Creating and copying machine config")
    user_wd = f"{args.mount_file}/{args.user}"
    cornflakes_dir = f"{args.mount_file}/{args.user}/cornflakes"
    tmp_dir = f"{args.mount_file}/{args.user}/cornflakes_tmp"
    config_dir = f"{args.mount_file}/{args.user}/config"
    config_file = f"{args.mount_file}/{args.user}/config/cluster_config.yaml"
    server_conn = machine_conns["server"]
    yaml = {}

    # get PCI address of NIC. Assumes all machines have same NIC.
    yaml_dpdk = {}
    res = server_conn.run(f"ethtool -i {args.iface_name} | grep 'bus-info'",
            quiet = True)
    pci_addr = res.stdout.strip().split(": ")[1]
    print(pci_addr)
    yaml_dpdk["eal_init"] = [f"-c", f"0xff", f"-n", f"8",
            f"-a",f"{pci_addr}",f"--proc-type=auto"]
    yaml_dpdk["pci_addr"] = f"{pci_addr}"
    yaml_dpdk["port"] = 0
    yaml["dpdk"] = yaml_dpdk

    yaml_mlx5 = {}
    yaml_mlx5["pci_addr"] = f"{pci_addr}"
    yaml["mlx5"] = yaml_mlx5
    
    yaml["port"] = 54321
    yaml["client_port"] = 12345

    # write host types
    yaml_clients = ["client{}".format(i) for i in range(1, len(args.clients) + 1)]
    yaml_server = ["server"]
    yaml["host_types"] = {"server": yaml_server, "client": yaml_clients}

    # get ethernet address of each host
    lwip_known_hosts = {}
    hosts = {}
    for machine_name, conn in machine_conns.items():
        ether_addr = conn.run(f"ifconfig {args.iface_name} | grep 'ether'",
                quiet = True).stdout.strip().split(" ")[1]
        private_ip = conn.run(f"ifconfig {args.iface_name} | grep 'inet'",
                quiet = True).stdout.strip().split(" ")[1]
        print(ether_addr)
        print(private_ip)
        addr = conn.get_addr()
        lwip_known_hosts[ether_addr] = private_ip
        hosts[machine_name] = {"addr": addr, "ip": private_ip, "mac":
                f"{ether_addr}", "tmp_folder": f"{tmp_dir}", "cornflakes_dir":
                f"{cornflakes_dir}", "config_file":
                f"{config_file}"}

    yaml["lwip"] = {"known_hosts": lwip_known_hosts}
    yaml["hosts"] = hosts

    # write experiment info if control machine provided, otherwise use this
    # machine
    yaml["max_clients"] = len(args.clients)
    yaml["user"] = args.user
    if args.control_key_path and (args.control_machine == "server" or
            args.control_machine in [f"client{i}" for i in range(1,
                len(args.clients))]):
        yaml["cornflakes_dir"] = f"{cornflakes_dir}"
        yaml["key"] = f"{args.control_key_path}"
    elif args.outfile is not None:
        yaml["cornflakes_dir"] = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        yaml["key"] = args.cloudlab_key
        with open(args.outfile, 'w') as outfile:
            pyyaml.dump(data, outfile, default_flow_style=False)

    # write to config file
    agenda.subtask(f"[{machine_name}] writing to {config_file}")

    yaml_string = pyyaml.dump(yaml, default_flow_style = False)
    for machine_name, conn in machine_conns.items():
        conn.run(f"echo \"{yaml_string}\"", stdout = config_file, quiet = True)

def reset_network(args, machine_name, machine_ip, machine_conns):
    # reset network for machine
    conn = machine_conns[machine_name]
    private_ip = "192.168.1.1"
    if "client" in machine_name:
        client_num = int(machine_name[6:])
        end = client_num + 1
        private_ip = f"192.168.1.{end}"

    # readd the network
    conn.run(f"ip link set dev {args.iface_name} up", 
            quiet = False, 
            sudo = True)
    conn.run(f"ip addr add dev {args.iface_name} {private_ip}/24 brd +",
            quiet = False, 
            sudo = True)
    conn.run(f"ip link set dev {args.iface_name} mtu 9000", 
            quiet =
            False, 
            sudo = True)

def run_mellanox_install(args, machine_name, machine_ip, machine_conns):
    agenda.task(f"[{machine_name}: {machine_ip}] run mellanox install")
    cornflakes_dir = f"{args.mount_file}/{args.user}/cornflakes"
    packages_dir = f"{args.mount_file}/{args.user}/packages"
    ofed_dir = f"{args.mount_file}/{args.user}/packages/MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64"
    conn = machine_conns[machine_name]
    conn.run("wget"\
    " https://content.mellanox.com/ofed/MLNX_OFED-5.6-2.0.9.0/MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.tgz"\
            " --no-check-certificate", wd = packages_dir, quiet = True)
    conn.run("tar -xzf MLNX_OFED_LINUX-5.6-2.0.9.0-ubuntu20.04-x86_64.tgz",
            wd = packages_dir,
            quiet = True)
    conn.run("./mlnxofedinstall --upstream-libs --dpdk --force",
            sudo = True,
            wd = ofed_dir,
            quiet = False,
            background = True)
    
def setup_huge_pages_and_cstates(args, machine_name, machine_ip, machine_conns):
    agenda.task(f"[{machine_name}: {machine_ip}] install huge pages")
    cornflakes_dir = f"{args.mount_file}/{args.user}/cornflakes"
    conn = machine_conns[machine_name]
    #conn.run("/etc/init.d/openibd restart",
    #            sudo = True,
    #            quiet = False)
    conn.run(f"{args.mount_file}/{args.user}/cornflakes/install/install-hugepages.sh 5192", wd = cornflakes_dir)
    # disable c-states and optionally try to disable CPU frequency changes
    conn.run(f"{args.mount_file}/{args.user}/cornflakes/install/set_freq.sh", wd = cornflakes_dir)

if __name__ == "__main__":
    print(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    main()

