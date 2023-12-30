use std::sync::{Arc, Mutex};

use crate::types::*;
use super::{PAGE_RECORD_BYTE, RecordPage, SlotNo, RecordId, HeapFile};

pub struct RawFileScan {
    heap_file: Arc<Mutex<HeapFile>>,
    status: ScanStatus,
}

enum ScanStatus {
    Starting,
    Scanning(RecordId),
    Finished
}

impl RawFileScan {
    pub fn new(heap_file: Arc<Mutex<HeapFile>>) -> RawFileScan {
        RawFileScan {
            heap_file,
            status: ScanStatus::Starting,
        }
    }

    pub fn with_record_page<F, T>(&self, page_id: PageId, f: F) -> Res<T> 
    where F: FnOnce(&mut RecordPage) -> Res<T> {
        let hf = self.heap_file.lock().unwrap();
        hf.with_record_page(page_id, f)
    }

    pub fn peer_next_rid(&mut self) -> Res<Option<RecordId>> {
        match self.status {
           ScanStatus::Starting => self.init_rid(),
           ScanStatus::Finished => Ok(None),
           ScanStatus::Scanning(rid) => {
                self.next_rid(rid)
           }
        }
    }

    pub fn get_next(&mut self) -> Res<Option<(RecordId, [u8; PAGE_RECORD_BYTE])>> {
        let rid = match self.status {
            ScanStatus::Starting => {
                if let Some(rid) = self.init_rid()? {
                    self.status = ScanStatus::Scanning(rid);
                    Some(rid)
                } else { 
                    self.status = ScanStatus::Finished;
                    None 
                }
            }
            ScanStatus::Finished => None,
            ScanStatus::Scanning(rid) => {
                if let Some(rid) = self.next_rid(rid)? {
                    self.status = ScanStatus::Scanning(rid);
                    Some(rid)
                } else {
                    self.status = ScanStatus::Finished;
                    None
                }
            }
        };
        match rid {
            None => Ok(None),
            Some(rid) => {
                let hf = self.heap_file.lock().unwrap();
                let rec = hf.get_record(rid)?;
                Ok(Some((rid, rec)))
            }
        }
    }

    fn init_rid(&mut self) -> Res<Option<RecordId>> {
        let page_id = {
            let hf = self.heap_file.lock().unwrap();
            let page_id = hf.with_header_page(|page| {
                page.get_head_free_page_id(hf.get_entry_no())
            })?;
            page_id
        };
        self.next(page_id, None)
    }

    fn next_rid(&mut self, rid: RecordId) -> Res<Option<RecordId>> {
        self.next(rid.page_id, Some(rid.slot_no))
    }

    fn next(&mut self, page_id: PageId, slot_no: Option<SlotNo>) -> Res<Option<RecordId>> {
        enum R {
            RecordId(RecordId),
            PageId(PageId),
            None,
        }
        let mut slot_no = match slot_no {
            Some(slot_no) => slot_no.value + 1,
            None => 0,
        };

        let r = self.with_record_page(page_id,
            |page| {
                let num_slots = page.get_num_slots()?;
                while slot_no < num_slots {
                    if !page.is_free_slot(SlotNo::new(slot_no))? {
                        return Ok(R::RecordId(RecordId {
                            page_id,
                            slot_no: SlotNo::new(slot_no),
                        }));
                    }
                    slot_no += 1;
                }
                let next_page_id = page.get_next_page_id()?;
                if next_page_id == 0 {
                    Ok(R::None)
                } else {
                    Ok(R::PageId(next_page_id))
                }
            }
        )?;
        match r {
            R::RecordId(rid) => Ok(Some(rid)),
            R::PageId(pid) => self.next(pid, None),
            R::None => Ok(None),
        }

        
    }
}


#[cfg(test)]
mod tests {

    use crate::{diskmgr::DiskMgr, bufmgr::BufMgr, filemgr::HFileMgr};

    use super::*;

    #[test]
    fn test_merge_sort() -> Res<()> {
        let name = "nano-raw-file-scan.db";
        let diskmgr = DiskMgr::open_db(name)?;
        let bufmgr = BufMgr::new(10, diskmgr);
        let bufmgr = Arc::new(Mutex::new(bufmgr));
        let mut filemgr = HFileMgr::build(bufmgr)?;
    
        let mut file = filemgr.create_file("file0")?;

        for i in 0..10 {
            let mut data = [0; PAGE_RECORD_BYTE];
            data[1] = i;
            file.insert_record(data)?;
        }

        let file = Arc::new(Mutex::new(file));
        let mut scan = RawFileScan::new(file);
        scan.get_next()?;
        scan.get_next()?;
        assert_eq!(2, scan.peer_next_rid()?.unwrap().slot_no.value);
        assert_eq!(2, scan.get_next()?.unwrap().0.slot_no.value);
        while let Some(x) = scan.peer_next_rid()? {
            println!("{}", x);
            scan.get_next()?;
        }
        Ok(())
    }

}