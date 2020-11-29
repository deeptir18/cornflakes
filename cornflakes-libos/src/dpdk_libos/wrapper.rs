use super::super::dpdk_call;
use super::{dpdk_bindings::*, dpdk_ok};
use color_eyre::eyre::{bail, Result};
use std::ffi::CString;
use std::fs::read_to_string;
use std::path::Path;
use tracing::info;
use yaml_rust::{Yaml, YamlLoader};

fn dpdk_eal_init(config_path: &str) -> Result<()> {
    let file_str = read_to_string(Path::new(&config_path))?;
    let yamls = match YamlLoader::load_from_str(&file_str) {
        Ok(docs) => docs,
        Err(e) => {
            bail!("Could not parse config yaml: {:?}", e);
        }
    };

    let mut args = vec![];
    let mut ptrs = vec![];
    let yaml = &yamls[0];
    match yaml["dpdk"].as_hash() {
        Some(map) => {
            let eal_init = match map.get(&Yaml::from_str("eal_init")) {
                Some(list) => list,
                None => {
                    bail!("Yaml config dpdk has no eal_init entry");
                }
            };
            info!("Eal init: {:?}", eal_init);
            for entry in eal_init.as_vec().unwrap() {
                //let entry_str = std::str::from_utf8(entry).unwrap();
                let s = CString::new(entry.as_str().unwrap()).unwrap();
                ptrs.push(s.as_ptr() as *mut u8);
                args.push(s);
            }
        }

        None => {
            bail!("Yaml config file has no entry dpdk");
        }
    }
    info!("DPDK init args: {:?}", args);
    dpdk_call!(rte_eal_init(ptrs.len() as i32, ptrs.as_ptr() as *mut _,));
    Ok(())
}

pub fn dpdk_init(config_path: &str) -> Result<()> {
    dpdk_eal_init(config_path)?;
    Ok(())
}
