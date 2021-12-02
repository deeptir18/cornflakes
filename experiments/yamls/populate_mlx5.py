#!/usr/bin/python3

import json
import yaml
from pathlib import Path
import sys

def main():
    action = None
    if len(sys.argv) > 1:
        action = sys.argv[1]
        print(action)

    actions = ['start', 'hosts', 'perf', 'log', 'stop']
    if action not in actions:
        print ('actions:', actions)
        exit(1)
        
    if action == 'start':
        yaml_file = 'mlx5-netperf.yaml'
        config_yaml = yaml.load(Path(yaml_file).read_text())
        config_values = 'server_config.json'
        config_dict = {}
        with open(config_values, 'r') as f:
            config_dict = json.load(f)
        
        start_cmd = config_yaml['programs']['start_server']['start']
        start_cmd = start_cmd.format(**config_dict)
        print(start_cmd)

if __name__=='__main__':
    main()
