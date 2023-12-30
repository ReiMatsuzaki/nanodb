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

    pub fn build_default(db_name: &str) -> Res<Self> {
        let diskmgr = DiskMgr::open_db(db_name)?;
        let bufmgr = BufMgr::new(10, diskmgr);
        let bufmgr = Arc::new(Mutex::new(bufmgr));
        let filemgr = HFileMgr::build(bufmgr)?;
        Ok(filemgr)
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
        let (page_id, _) = bufmgr.create_page()?;
        Ok(page_id)
    }

    pub fn create_file(&mut self, name: &str) -> Res<HeapFile> {
        if let Some(_) = self.find_file(name)? {
            return Err(Error::InvalidArg { 
                msg: format!(
                    "HFileMgr::create_file: file already exists. name={}",
                    name)
            })
        }
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

    pub fn open(&mut self, name: &str) -> Res<HeapFile> {
        match self.find_file(name)? {
            None => self.create_file(name),
            Some(entry_no) => {
                let bufmgr = self.bufmgr.clone();
                Ok(HeapFile::new(entry_no, bufmgr))
            }
        }
    }

    fn find_file(&mut self, name: &str) -> Res<Option<EntryNo>> {
        self.with_header_page(|header_page| {
            header_page.find(name)
        })
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

pub fn with_records_pages<F>(f: F, first_page_id: PageId, mutex: &Arc<Mutex<BufMgr>>) -> Res<()> 
where F: FnMut(PageId, &mut RecordPage) -> Res<()> {
    let mut pid = first_page_id;
    let mut f = f;
    while pid > 0 {
        let mut bufmgr = mutex.lock().unwrap();
        let page = bufmgr.pin_page(pid)?;
        let mut page = RecordPage::new(page);

        let old_pid = pid;
        pid = page.get_next_page_id()?;

        f(old_pid, &mut page)?;
        bufmgr.unpin_page(old_pid)?;
    }
    Ok(())
}

// pub fn with_two_record_pages<F, T>(f: F, page_id_0: PageId, page_id_1: PageId, mutex: &Arc<Mutex<BufMgr>>) -> Res<T>
// where F: FnOnce(&mut RecordPage, &mut RecordPage) -> Res<T> {
//     let mut bufmgr = mutex.lock().unwrap();
//     let p0 = bufmgr.pin_page(page_id_0)?;
//     let p1 = bufmgr.pin_page(page_id_1)?;
//     let mut page0 = RecordPage::new(p0);
//     let mut page1 = RecordPage::new(p1);
//     let res = f(&mut page0, &mut page1);
//     bufmgr.unpin_page(page_id_0)?;
//     bufmgr.unpin_page(page_id_1)?;
//     res
// }

pub fn create_page(mutex: &Arc<Mutex<BufMgr>>) -> Res<PageId> {
    let mut bufmgr = mutex.lock().unwrap();
    let (page_id, _) = bufmgr.create_page()?;
    Ok(page_id)
}

pub fn run_hfilemgr() -> Res<()> {
    let name = "nano-hfilemgr.db";
    // // let diskmgr = DiskMgr::open_db(name)?;
    // // let bufmgr = BufMgr::new(10, diskmgr);
    // // let bufmgr = Arc::new(Mutex::new(bufmgr));
    // let mut hfilemgr = HFileMgr::build(bufmgr)?;
    let mut hfilemgr = HFileMgr::build_default(name)?;

    println!("create heap file");
    let mut file_a = hfilemgr.create_file("file_a")?;
    println!("eno={:}", file_a.get_entry_no().value);
    let mut data: [u8; PAGE_RECORD_BYTE] = [0; PAGE_RECORD_BYTE];
    for i in 0..10 {
        data[i] = 10 + i as u8 + 1;
    }
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
    let mut data: [u8; PAGE_RECORD_BYTE] = [0; PAGE_RECORD_BYTE];
    for i in 0..10 {
        data[i] = i as u8 + 1;
    }
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