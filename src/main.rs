use core::time;
use std::collections::HashMap;
use std::fs::File;
use std::io::{Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

extern crate pretty_env_logger;
#[macro_use]
extern crate log;

mod test;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const PKG_NAME: &str = env!("CARGO_PKG_NAME");

const SEGMENT_DIR: &str = "./db/";
const SEGMENT_TMP_IDX: usize = 0;
const SEGMENT_SIZE_MAX: usize = 50;

#[derive(Debug)]
struct DB {
    pub segment_tmp: Arc<Mutex<File>>,
    segment_index: Vec<HashMap<String, u64>>,
}

impl DB {
    pub fn init() -> DB {
        let mut segment_index = Vec::new();
        segment_index[SEGMENT_TMP_IDX] = HashMap::new();

        // todo load segments from disk

        let mut segment_tmp =
            tempfile::tempfile_in(SEGMENT_DIR).expect("unable to create temporary segment file");

        let mutex = Arc::new(Mutex::new(segment_tmp));

        DB {
            segment_tmp: mutex,
            segment_index: segment_index,
        }
    }
    pub fn set(&mut self, key: String, value: String) {
        let mut segment = self.segment_tmp.lock().unwrap();
        let idx = segment.metadata().unwrap().len();
        writeln!(segment, "{value}"); // todo does this write to where we want?

        self.segment_index[SEGMENT_TMP_IDX].insert(key, idx);
    }

    pub fn get(&mut self, key: String) -> Result<String, Error> {
        // todo check in all segments

        match self.segment_index[SEGMENT_TMP_IDX].get(&key) {
            Some(index) => {
                let mut segment = self.segment_tmp.lock().unwrap();
                segment.seek(SeekFrom::Start(*index)).unwrap();

                let mut buf = String::new();

                segment.read_to_string(&mut buf).unwrap();
                return Ok(buf.lines().take(1).collect());
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found")),
        }
    }

    pub fn handle_segments(self) {
        loop {
            thread::sleep(time::Duration::from_millis(1000));
            debug!("handling segments");
        }
    }
}

fn handle_request(mut stream: TcpStream) {
    info!("connected client: {}", stream.peer_addr().unwrap());

    let mut data = [0; 1024];

    match stream.read(&mut data) {
        Ok(_) => {
            let message = String::from_utf8(data.to_vec()).unwrap();
            info!("received message from client: '{message}'");

            stream.write_all(b"processing request\n").unwrap();

            thread::sleep(time::Duration::from_millis(1000));

            stream.write_all(b"request done\n").unwrap();
            stream.flush().unwrap(); // todo need?

            stream.shutdown(Shutdown::Both).unwrap();
        }
        Err(e) => {
            eprintln!("{}", e);
        }
    }
}

fn main() {
    pretty_env_logger::formatted_builder()
        .filter_level(log::LevelFilter::Debug)
        .init();

    info!("running '{}' with version '{}'", PKG_NAME, VERSION);

    let mut db = DB::init();

    thread::spawn(move || {
        db.handle_segments();
    });

    let listener = TcpListener::bind("0:7654").expect("unable to bind to socket");

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                // TODO add a thread pool
                thread::spawn(move || {
                    handle_request(stream);
                });
            }
            Err(e) => {
                error!("Error: {}", e);
            }
        }
    }
}
