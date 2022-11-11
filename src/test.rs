use std::collections::HashMap;

use super::*;

#[test]
fn test_gets_one() {
    let mut segment_tmp = tempfile::tempfile().expect("unable to create temporary segment file");

    let mutex = Arc::new(Mutex::new(segment_tmp));

    let mut db = DB {
        segment_tmp: mutex,
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
