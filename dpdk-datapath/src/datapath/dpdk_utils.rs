use color_eyre::eyre::{bail, Result};
use std::{fs::read_to_string, path::Path};
use yaml_rust::{Yaml, YamlLoader};

pub fn parse_eal_init(config_path: &str) -> Result<Vec<String>> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let mut args = vec![];
    let yaml = &yamls[0];
    match yaml["dpdk"].as_hash() {
        Some(map) => {
            let eal_init = match map.get(&Yaml::from_str("eal_init")) {
                Some(list) => list,
                None => {
                    bail!("Yaml config dpdk has no eal_init entry");
                }
            };
            for entry in eal_init.as_vec().unwrap() {
                //let entry_str = std::str::from_utf8(entry).unwrap();
                args.push(entry.as_str().unwrap().to_string());
            }
        }

        None => {
            bail!("Yaml config file has no entry dpdk");
        }
    }
    Ok(args)
}
