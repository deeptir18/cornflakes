# Experiment Repo for Cornflakes


## Dependencies
All experiments depend on the main Cornflakes repo already being compiled.

### Python Packages
Please install the following Python packages:
```
pip3 install colorama
pip3 install fabric
```

## Experiments

### Scatter-Gather Experiments
```
python3 -e [individual,loop] -f <RESULTS_PATH> -c <CONFIG_YAML> -ec $CORNFLAKES_DIR/scatter-gather-bench/experiment.yaml
```
