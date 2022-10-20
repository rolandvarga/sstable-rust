use std::collections::HashMap;
use std::io::{Error, ErrorKind};

#[derive(Debug)]
struct DB {
    pub data: String,
    hashIndex: HashMap<String, usize>, // TODO should index contain length?
}

impl DB {
    pub fn set(&mut self, key: String, value: String) {
        let s = format!("{},{}\n", key, value);
        self.data.push_str(s.as_str());

        // get index of s
        let index = self.data.len() - s.len();
        self.hashIndex.insert(key, index);
    }

    // look inside the hashmap, get the byte offset & return the value
    pub fn get(&mut self, key: String) -> Result<String, Error> {
        match self.hashIndex.get(&key) {
            Some(index) => {
                let mut end: usize = index + 1;

                // decimal 10 == \n
                while end < self.data.len() && self.data.as_bytes()[end] != 10 {
                    end += 1;
                }

                let pairs = self.data[*index..end].to_string();
                let val = pairs.split(",").collect::<Vec<&str>>()[1].to_string();

                return Ok(val);
            }
            None => Err(Error::new(ErrorKind::NotFound, "key not found")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gets_one() {
        let mut db = DB {
            data: String::new(),
            hashIndex: HashMap::new(),
        };

        db.set("tomato".to_string(), "235".to_string());
        db.set("orange".to_string(), "187".to_string());
        db.set("apple".to_string(), "125".to_string());

        println!("{:?}", db);

        match db.get("apple".to_string()) {
            Ok(value) => assert_eq!(value, "125"),
            Err(e) => assert_eq!(e.kind(), ErrorKind::NotFound),
        }
    }
}

fn main() {}
