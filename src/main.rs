use std::fs::File;
use std::io::{BufReader, Read};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(name = "wc-rs", about = "Word count!")]
struct Opt {
    /// Input file
    #[structopt(parse(from_os_str))]
    input: PathBuf,
}

const NEW_LINE: u8 = '\n' as u8;

#[cfg(not(feature = "parallel"))]
pub fn count(input: &PathBuf) -> usize {
    let file = File::open(input).expect("Count not open file.");
    let reader = BufReader::new(file);
    reader.bytes().into_iter().fold(0, |acc, byte| match byte {
        Ok(byte) => {
            if byte == NEW_LINE {
                acc + 1
            } else {
                acc
            }
        }
        Err(_) => acc,
    })
}

#[cfg(feature = "parallel")]
pub fn count(input: &PathBuf) -> usize {
    use rayon::prelude::*;
    use std::io::{Seek, SeekFrom};

    let file_size_bytes = std::fs::metadata(&input).unwrap().len();
    let num_chunks = num_cpus::get() as u64;
    let chunk_size = file_size_bytes / num_chunks;

    let readers = (0..num_chunks).map(|index| {
        let mut file = File::open(&input).expect("Count not open file.");
        file.seek(SeekFrom::Start(chunk_size * index))
            .expect("Failed seek.");
        let reader = BufReader::new(file);
        let n = if index == num_chunks - 1 {
            chunk_size + file_size_bytes % num_chunks
        } else {
            chunk_size
        } as usize;
        reader.bytes().take(n)
    });

    readers
        .par_bridge()
        .fold_with(0, |acc, reader| {
            reader.fold(acc, |acc, byte| match byte {
                Ok(byte) => {
                    if byte == NEW_LINE {
                        acc + 1
                    } else {
                        acc
                    }
                }
                Err(_) => acc,
            })
        })
        .sum()
}

fn main() {
    let opt = Opt::from_args();
    let count = count(&opt.input);
    println!("{} {}", count, opt.input.to_str().unwrap());
}
