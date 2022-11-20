# Setting up Cornflakes experiments on Cloudlab

## Requirements.
Cornflakes experiments require you have one machine that can be used to
run experiments from.
This can be one of the cloudlab machines themselves, or
the machine you ssh into cloudlab from.
This script handles setting up all experiment dependencies if one of the
cloudlab machines themselves will be used to run experiments, but it is
theoretically possible to run experiments from any machine that has ssh access into the cloudlab machines.


## Requirements on this machine.
To run `setup-cloudlab.py`, it is necessary to install a few python
libraries on the machine you use to ssh into cloudlab.
Run (from the cornflakes directory):
```
./install/cloudlab/setup-dependencies.sh
```
If this machine will be used to run Cornflakes experiments, ensure the
following Python and R packages are installed on this machine. Skip if you will run Cornflakes experiments from one of the cloudlab nodes itself.
```
# Python dependencies
pip3 install colorama gitpython tqdm parse numpy pandas torch 
pip3 install agenda toml result
pip3 install setuptools_rust
pip3 install fabric==2.6.0
pip3 install pyelftools

# R
# ggplot-2,

# library(ggplot2)
# library(plyr)
# library(tidyr)
# library(extrafont)
# library(showtext)
# library(viridis)
```

## Running setup-cloudlab.py
The cloudlab setup script has four functions: 
1. Basic setup (installs required packages, clones cornflakes, sets up a mount on the remote machine for all packages and repos)
2. Generate network config (generates Cornflakes config file)
3. Installs mellanox drivers
4. Sets up reboot state (huge pages, cstates).

## Recommended way to install
Note that for c6525-100g machines on cloudlab, iface name before mellanox
install is `ens1f0` and is `ens1f0np0` after mellanox install.
0. If using a Cloudlab node as the experiment base, ensure that machine has
   ssh access to the other machines
1. Install the basic dependencies:
```
python3 setup-cloudlab.py \
--server $SERVER_MACHINE_IP \
--client $CLIENT1_IP $CLIENT2_IP \
--user <username> \
--cloudlab_key <path/to/ssh/key/for/cloudlab \
--mount_file <where filesystem will be mounted for cloudlab> \
--clone_with_ssh # whether to clone cornflakes with ssh \
--iface_name <cloudlab iface name> \
--control_machine server \
--control_key_path <path/to/server/ssh/key> \
--basic_setup
```

2. Create the configuration file:
```
python3 setup-cloudlab.py \
--server $SERVER_MACHINE_IP \
--client $CLIENT1_IP $CLIENT2_IP \
--user <username> \
--cloudlab_key <path/to/ssh/key/for/cloudlab \
--mount_file <where filesystem will be mounted for cloudlab> \
--clone_with_ssh # whether to clone cornflakes with ssh \
--iface_name <cloudlab iface name> \
--control_machine server \
--control_key_path <path/to/server/ssh/key> \
--generate_config
```

3. Run the Mellanox install
This will likely gork the machine and change the interface names.
```
python3 setup-cloudlab.py \
--server $SERVER_MACHINE_IP \
--client $CLIENT1_IP $CLIENT2_IP \
--user <username> \
--cloudlab_key <path/to/ssh/key/for/cloudlab \
--mount_file <where filesystem will be mounted for cloudlab> \
--clone_with_ssh # whether to clone cornflakes with ssh \
--iface_name <cloudlab iface name> \
--control_machine server \
--control_key_path <path/to/server/ssh/key> \
--mlx5
```

4. If the machines were gorked, you will have to manually reboot the
   machine on cloudlab.

5. Log into the machine for the new interface name, and now run:
```
python3 setup-cloudlab.py \
--server $SERVER_MACHINE_IP \
--client $CLIENT1_IP $CLIENT2_IP \
--user <username> \
--cloudlab_key <path/to/ssh/key/for/cloudlab \
--mount_file <where filesystem will be mounted for cloudlab> \
--clone_with_ssh # whether to clone cornflakes with ssh \
--iface_name <cloudlab iface name> \
--control_machine server \
--control_key_path <path/to/server/ssh/key> \
--reset-network
```

6. Finally, run the setup script for reboot state (hugepages, cstates):
```
python3 setup-cloudlab.py \
--server $SERVER_MACHINE_IP \
--client $CLIENT1_IP $CLIENT2_IP \
--user <username> \
--cloudlab_key <path/to/ssh/key/for/cloudlab \
--mount_file <where filesystem will be mounted for cloudlab> \
--clone_with_ssh # whether to clone cornflakes with ssh \
--iface_name <cloudlab iface name> \
--control_machine server \
--control_key_path <path/to/server/ssh/key> \
--reboot-state
```




