pub mod relmgr;
pub mod schema;
pub mod record;
pub mod file_scan;

use std::sync::{Arc, Mutex};

pub use relmgr::*;
pub use schema::*;
pub use record::*;
pub use file_scan::*;

use super::types::Res;
use super::diskmgr::DiskMgr;
use super::bufmgr::BufMgr;
use super::filemgr::{FileMgr, HFileMgr};

pub fn run_relmgr() -> Res<()> {
    let name = "nano-relmgr-2.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let bufmgr = Arc::new(Mutex::new(bufmgr));
    let mut filemgr = HFileMgr::build(bufmgr)?;

    let mut file0 = filemgr.create_file("file0")?;
    let data = [4, 0, 0, 0, 75, 76, 77, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for i in 0..15 {
        let rid = file0.insert_record(data)?;
        println!("insert {}, {}", i, rid);
    }

    let schema = Schema::new(vec![
        ("id".to_string(), AttributeType::Int),
        ("name".to_string(), AttributeType::Varchar(3)),
    ]);

    let file0 = Arc::new(Mutex::new(file0));
    let mut scan = FileScan::new(file0.clone(), schema);
    while let Some((rid, rec)) = scan.get_next()? {
        println!("{}: {}", rid, rec);
    }

    std::fs::remove_file(name).unwrap();
    Ok(())
}

pub fn run_relmgr_old() -> Res<()> {
    let name = "nano-relmgr.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let mut filemgr = FileMgr::build(bufmgr)?;

    let eno = filemgr.create_file("test")?;
    let data = [4, 0, 0, 0, 75, 76, 77, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let num = 12;
    for i in 0..num {
        println!("insert {}", i);
        filemgr.insert_record(eno, data)?;
    }

    let schema = Schema::new(vec![
        ("id".to_string(), AttributeType::Int),
        ("name".to_string(), AttributeType::Varchar(3)),
    ]);
    let mut file_scan = OldFileScan::build(schema, filemgr, eno)?;
    if let Some(rec) = file_scan.get_next()? {
        assert_eq!(75, *rec.get_byte(4).unwrap());
        let a = rec.get_int_field(0).unwrap();
        assert_eq!(a, 4);
        let b = rec.get_varchar_field(1).unwrap();
        assert_eq!("KLM".to_string(), b);
        rec.print();
    }

    while let Some(rec) = file_scan.get_next()? {
        rec.print();
    }
    file_scan.reset()?;

    std::fs::remove_file(name).unwrap();
    Ok(())
}