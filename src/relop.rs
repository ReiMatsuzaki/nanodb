pub mod schema;
pub mod record;
pub mod file_scan;
pub mod projection;
pub mod merge_sort;

use std::sync::{Arc, Mutex};

pub use schema::*;
pub use record::*;
pub use file_scan::*;
pub use projection::*;
pub use merge_sort::*;

use crate::filemgr::PAGE_RECORD_BYTE;

use super::types::Res;
use super::diskmgr::DiskMgr;
use super::bufmgr::BufMgr;
use super::filemgr::HFileMgr;

pub fn run_relmgr() -> Res<()> {
    let name = "nano-relmgr-2.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let bufmgr = Arc::new(Mutex::new(bufmgr));
    let mut filemgr = HFileMgr::build(bufmgr)?;

    let mut file0 = filemgr.create_file("file0")?;
    let mut data = [0; PAGE_RECORD_BYTE];
    // let d = [4, 0, 0, 0, 75, 76, 77, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    data[3] = 4;
    data[4] = 75;
    data[5] = 76;
    data[6] = 77;
    for i in 0..15 {
        let rid = file0.insert_record(data)?;
        println!("insert {}, {}", i, rid);
    }

    let schema = Schema::build(vec![
        ("id".to_string(), AttributeType::Int),
        ("name".to_string(), AttributeType::Varchar(4)),
    ]);

    let file0 = Arc::new(Mutex::new(file0));
    let mut count = 0;
    let mut scan = FileScan::new(file0.clone(), schema);
   
    let (_, rec) = scan.get_next()?.unwrap();
    count += 1;
    assert_eq!(4, rec.get_int_field(0).unwrap());
    assert_eq!("KLM", rec.get_varchar_field(1).unwrap());

    while let Some((rid, rec)) = scan.get_next()? {
        println!("{}: {}", rid, rec);
        count += 1;
    }
    assert_eq!(15, count);

    std::fs::remove_file(name).unwrap();
    Ok(())
}

pub fn run_relmgr_projection() -> Res<()> {
    let name = "nano-relmgr-3.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let bufmgr = Arc::new(Mutex::new(bufmgr));
    let mut filemgr = HFileMgr::build(bufmgr)?;

    let mut file0 = filemgr.create_file("file0")?;
    let schema = Schema::build(vec![
        ("id".to_string(), AttributeType::Int),
        ("name".to_string(), AttributeType::Varchar(4)),
    ]);
    let mut rec = Record::new_zero(&schema);
    rec.set_int_field(0, 4)?;
    rec.set_varchar_field(1, &"KVM".to_string())?;
    let data = rec.get_data();
    // let data = [4, 0, 0, 0, 75, 76, 77, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for i in 0..2 {
        let rid = file0.insert_record(*data)?;
        println!("insert {}, {}", i, rid);
    }

    let file0 = Arc::new(Mutex::new(file0));
    let file_scan = FileScan::new(file0.clone(), schema);
    let fnos = vec![1];
    let mut iterator = Projection::build(file_scan, fnos)?;
   
    let (_, rec) = iterator.get_next()?.unwrap();
    assert_eq!(1, rec.get_field_len());
    assert_eq!("KVM", rec.get_varchar_field(0).unwrap());

    while let Some((rid, rec)) = iterator.get_next()? {
        println!("{}: {}", rid, rec);
    }

    std::fs::remove_file(name).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_relmgr() {
        run_relmgr().unwrap();
    }

    #[test]
    fn test_relmgr_projection() {
        run_relmgr_projection().unwrap()
    }
}