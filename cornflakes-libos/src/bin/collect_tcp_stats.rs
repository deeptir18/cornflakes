use std::env;
use std::fs;
// use serde_json::to_writer;
use color_eyre::eyre::Result;
use cornflakes_libos::loadgen::client_threads::dump_thread_stats;
use cornflakes_libos::loadgen::client_threads::ThreadStats;
use cornflakes_libos::timing::ManualHistogram;
use serde_json::Value;

pub fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    println!("{:?}", args);
    if args[1] == "--file-name" {
        let file_path = args[2].to_string();

        println!("In file {}", &file_path);

        let contents =
            fs::read_to_string(&file_path).expect("Should have been able to read the file");

        let latencies: Vec<u64> = contents
            .split_whitespace()
            .map(|x| x.parse::<u64>().unwrap())
            .collect();

        let mut histo: ManualHistogram = ManualHistogram::new(latencies.len());

        for val in latencies.into_iter() {
            histo.record(val);
        }

        let file_path_json = args[2].to_string() + ".1";

        println!("In file {}", &file_path_json);

        let json_fn = fs::read_to_string(&file_path_json).expect("Could not read file");
        let mut json_results: Value = serde_json::from_str(&json_fn).unwrap();

        let stats = ThreadStats::new(
            // thread_id as u16,
            0 as u16,
            json_results.get_mut("ipackets").unwrap().as_i64().unwrap() as usize,
            json_results.get_mut("opackets").unwrap().as_i64().unwrap() as usize,
            1,
            json_results.get_mut("duration").unwrap().as_i64().unwrap() as f64 * 1e9,
            (json_results.get_mut("opackets").unwrap().as_i64().unwrap() as f64
                / (json_results.get_mut("duration").unwrap().as_i64().unwrap() as f64))
                as u64,
            json_results
                .get_mut("frame_size")
                .unwrap()
                .as_i64()
                .unwrap() as usize,
            &mut histo,
            0 as usize,
        )
        .unwrap();

        dump_thread_stats(vec![stats], Some("output.json".to_owned()), true)?;

        if args.len() == 4 && args[3] == "--delete" {
            fs::remove_file(&file_path)?;
            fs::remove_file(&file_path_json)?;
        }
    }
    Ok(())
}
