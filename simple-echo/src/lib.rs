pub mod client;
pub mod server;

use color_eyre::eyre::{bail, Result};
use std::str::FromStr;

#[derive(Debug, PartialEq, Eq, Clone, Default)]
pub struct RequestShape {
    pattern: Vec<usize>, // Vector of sizes of entry
    num_repeats: usize,  // number of times pattern is repeated
}

impl FromStr for RequestShape {
    type Err = color_eyre::eyre::Error;
    /// RequestShape command line format:
    /// 1-4-256
    ///     first number = pattern length
    ///     second number = times to repeat pattern
    ///     next (pattern length) numbers = pattern (of sizes)
    ///     1-4-256 = pattern(256) repeated four times
    /// 2-2-256-1024
    ///     pattern(245, 1024) repeated four times
    fn from_str(s: &str) -> Result<RequestShape> {
        let split: Vec<&str> = s.split("-").collect();
        if split.len() < 3 {
            bail!(
                "Request shape pattern needs atleast 3 numbers, got: {:?}",
                split
            );
        }
        let pattern_length: usize = split[0].parse::<usize>()?;
        if split.len() != pattern_length + 2 {
            bail!("Invalid pattern length (first number)");
        }
        let num_repeats: usize = split[1].parse::<usize>()?;
        let mut pattern: Vec<usize> = Vec::with_capacity(pattern_length);
        for i in 2..(pattern_length + 2) {
            let size: usize = split[i].parse::<usize>()?;
            pattern.push(size);
        }
        Ok(RequestShape::new(num_repeats, pattern))
    }
}

impl RequestShape {
    pub fn new(num_repeats: usize, pattern: Vec<usize>) -> Self {
        RequestShape {
            pattern: pattern,
            num_repeats: num_repeats,
        }
    }

    pub fn total_data_len(&self) -> usize {
        let mut sum = 0;
        for _ in 0..self.num_repeats {
            for size in self.pattern.iter() {
                sum += *size;
            }
        }
        sum
    }

    pub fn range_vec(&self) -> Vec<(usize, usize)> {
        let mut ret: Vec<(usize, usize)> = Vec::default();
        // TODO: this code assumes the entire range fits in a single jumbo frame
        let mut offset = 0;
        for _ in 0..self.num_repeats {
            for size in self.pattern.iter() {
                ret.push((offset, *size));
                offset += size;
            }
        }
        ret
    }

    pub fn message_size(&self) -> usize {
        let mut sum: usize = 0;
        for _ in 0..self.num_repeats {
            for size in self.pattern.iter() {
                sum += *size;
            }
        }
        sum
    }

    // generate a series of bytes with a specific pattern.
    pub fn generate_bytes(&self) -> Vec<Vec<u8>> {
        let alphabet = "abcdefghijklmnopqrstuvwqyz";
        let mut ret: Vec<Vec<u8>> = Vec::default();
        let mut alph_index = 0;
        for _ in 0..self.num_repeats {
            for size in self.pattern.iter() {
                let l = alphabet.chars().nth(alph_index % alphabet.len()).unwrap();
                let chars: String = std::iter::repeat(())
                    .map(|()| l)
                    .map(char::from)
                    .take(*size)
                    .collect();
                ret.push(chars.as_str().as_bytes().to_vec());
                alph_index += 1;
            }
        }
        ret
    }
}
