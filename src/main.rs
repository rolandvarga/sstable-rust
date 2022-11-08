use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};

use tempfile;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

static VERSION: &str = env!("CARGO_PKG_VERSION");
static PKG_NAME: &str = env!("CARGO_PKG_NAME");

static SEGMENT_TMP_IDX: usize = 0;
static SEGMENT_SIZE_MAX: usize = 50;

#[derive(Debug)]
struct DB {
    pub data: File,
    segment_index: Vec<HashMap<String, u64>>,
}

impl DB {
    pub fn init() -> DB {
        let mut segment_index = Vec::new();
        segment_index[SEGMENT_TMP_IDX] = HashMap::new();

        // todo load segments from disk

        let mut tmp_segment =
            tempfile::tempfile().expect("unable to create temporary segment file");

        DB {
            data: tmp_segment,
            segment_index: segment_index,
        }
    }
    pub fn set(&mut self, key: String, value: String) {
        let idx = self.data.metadata().unwrap().len();
        writeln!(self.data, "{value}");

        self.segment_index[SEGMENT_TMP_IDX].insert(key, idx);
    }

    pub fn get(&mut self, key: String) -> Result<String, Error> {
        // todo check in all segments

        match self.segment_index[SEGMENT_TMP_IDX].get(&key) {
            Some(index) => {
                self.data.seek(SeekFrom::Start(*index)).unwrap();

                let mut buf = String::new();

                self.data.read_to_string(&mut buf).unwrap();
                return Ok(buf.lines().take(1).collect());
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found")),
        }
    }
}

fn main() {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("running '{}' with version '{}'", PKG_NAME, VERSION);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gets_one() {
        let mut tmp_segment =
            tempfile::tempfile().expect("unable to create temporary segment file");

        let mut db = DB {
            data: tmp_segment,
            segment_index: vec![HashMap::new()],
        };

        db.set("tomato".to_string(), "235".to_string());
        db.set("orange".to_string(), "187".to_string());
        db.set("apple".to_string(), "125".to_string());

        println!("{:?}", db);

        assert_eq!(db.get("tomato".to_string()).unwrap(), "235");
        assert_eq!(db.get("orange".to_string()).unwrap(), "187");
        assert_eq!(db.get("apple".to_string()).unwrap(), "125");
    }
}
