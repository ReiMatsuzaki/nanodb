use std::sync::{Arc, Mutex};

use crate::types::*;
use crate::bufmgr::BufMgr;
use super::heap_file::*;
use super::*;

pub struct HFileMgr {
    bufmgr: Arc<Mutex<BufMgr>>,
    header_page_id: PageId,
}

impl HFileMgr {
    pub fn build(bufmgr: Arc<Mutex<BufMgr>>) -> Res<Self> {
        let cloned = bufmgr.clone();
        let mut mgr = bufmgr.lock().unwrap();
        let (header_page_id, _) =mgr.create_page()?;
        let mgr = Self {
            bufmgr: cloned,
            header_page_id,
        };
        Ok(mgr)
    }

    fn with_header_page<F, T>(&self, f: F) -> Res<T>
    where F: FnOnce(&mut HeaderPage) -> Res<T> {
        with_header_page(f, &self.bufmgr)
    }

    fn with_record_page<F, T>(&self, page_id: PageId, f: F) -> Res<T> 
    where F: FnOnce(&mut RecordPage) -> Res<T> {
        with_record_page(f, page_id, &self.bufmgr)
    }

    fn create_page(&mut self) -> Res<PageId> {
        let mut bufmgr = self.bufmgr.lock().unwrap();
        println!("create page");
        let (page_id, _) = bufmgr.create_page()?;
        Ok(page_id)
    }

    pub fn create_file(&mut self, name: &str) -> Res<HeapFile> {
        let page_id = self.create_page()?;

        let entry_no = self.with_header_page(|header_page| {
            let entry_no = header_page.new_entry()?;
            header_page.set_head_free_page_id(entry_no, page_id)?;
            header_page.set_head_full_page_id(entry_no, 0)?;
            header_page.set_name(entry_no, name)?;
            Ok(entry_no)
        })?;

        // FIXME: summarize as new page procedure
        let hpid = self.header_page_id;
        self.with_record_page(page_id, |page| {
            page.set_next_page_id(0)?; // set invalid page id
            page.set_prev_page_id(hpid)?;
            Ok(())
        })?;

        let bufmgr = self.bufmgr.clone();
        Ok(HeapFile::new(entry_no, bufmgr))
    }
}

pub fn with_header_page<F, T>(f: F, mutex: &Arc<Mutex<BufMgr>>) -> Res<T>
where F: FnOnce(&mut HeaderPage) -> Res<T> {
    let header_page_id = 1;
    let mut bufmgr = mutex.lock().unwrap();
    let header_page = bufmgr.pin_page(header_page_id)?;
    let mut header_page = HeaderPage::new(header_page);
    let res = f(&mut header_page);
    bufmgr.unpin_page(header_page_id)?;
    res
}

pub fn with_record_page<F, T>(f: F, page_id: PageId, mutex: &Arc<Mutex<BufMgr>>) -> Res<T>
where F: FnOnce(&mut RecordPage) -> Res<T> {
    let mut bufmgr = mutex.lock().unwrap();
    let page = bufmgr.pin_page(page_id)?;
    let mut page = RecordPage::new(page);
    let res = f(&mut page);
    bufmgr.unpin_page(page_id)?;
    res
}

pub fn create_page(mutex: &Arc<Mutex<BufMgr>>) -> Res<PageId> {
    let mut bufmgr = mutex.lock().unwrap();
    let (page_id, _) = bufmgr.create_page()?;
    Ok(page_id)
}

pub fn run_hfilemgr() -> Res<()> {
    let name = "nano-hfilemgr.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let bufmgr = Arc::new(Mutex::new(bufmgr));
    let mut hfilemgr = HFileMgr::build(bufmgr)?;

    println!("create heap file");
    let mut file_a = hfilemgr.create_file("file_a")?;
    println!("eno={:}", file_a.get_entry_no().value);
    let data = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    for _ in 0..3 {
        file_a.insert_record(data)?;
    }
    let rid = file_a.insert_record(data)?;
    for _ in 0..3 {
        file_a.insert_record(data)?;
    }    
    println!("rid=({}, {})", rid.page_id, rid.slot_no.value);

    let rec = file_a.get_record(rid)?;
    println!("rec={:?}", rec);

    println!("delete record");
    file_a.delete_record(rid)?;

    println!("try to get deleted one");
    let a = file_a.get_record(rid);
    assert!(a.is_err());

    println!("insert other data");
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let rid = file_a.insert_record(data)?;
    println!("rid=({}, {})", rid.page_id, rid.slot_no.value);
    let rec = file_a.get_record(rid)?;
    assert_eq!(3, rec[2]);
    println!("rec={:?}", rec);

    println!("print all");
    let mutex = Arc::new(Mutex::new(file_a));
    let mut it = RawFileScan::new(mutex);
    while let Some((rid, rec)) = it.get_next()?{
        println!("({},{}): {:?}", rid.page_id, rid.slot_no.value, rec)
    }

    std::fs::remove_file(name).unwrap();
    Ok(())
}