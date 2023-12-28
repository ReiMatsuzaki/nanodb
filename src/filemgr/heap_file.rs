use std::sync::{Arc, Mutex};

use crate::types::{Res, EntryNo, PageId, Error};
use crate::bufmgr::BufMgr;
use super::{PAGE_RECORD_BYTE, HeaderPage, RecordPage, with_record_page, with_header_page, SlotNo, create_page, RecordId};

pub struct HeapFile {
    entry_no: EntryNo,
    bufmgr: Arc<Mutex<BufMgr>>,
}

impl HeapFile {
    pub fn new(entry_no: EntryNo, bufmgr: Arc<Mutex<BufMgr>>) -> HeapFile {
        HeapFile {
            entry_no,
            bufmgr,
        }
    }

    pub fn with_header_page<F, T>(&self, f: F) -> Res<T>
    where F: FnOnce(&mut HeaderPage) -> Res<T> {
        with_header_page(f, &self.bufmgr)
    }

    pub fn with_record_page<F, T>(&self, page_id: PageId, f: F) -> Res<T> 
    where F: FnOnce(&mut RecordPage) -> Res<T> {
        with_record_page(f, page_id, &self.bufmgr)
    }

    pub fn get_entry_no(&self) -> EntryNo { self.entry_no }

    pub fn insert_record(&mut self, data: [u8; PAGE_RECORD_BYTE]) -> Res<RecordId> {
        let page_id = self.with_header_page(|h| {
            h.get_head_free_page_id(self.entry_no)
        })?;
        self.insert_record_page(page_id, data)
    }

    fn insert_record_page(&mut self, page_id: PageId, data: [u8; PAGE_RECORD_BYTE]) -> Res<RecordId> {
        if page_id == 0 {
            return Err(Error::NoFreePage);
        }

        let success = self.with_record_page(page_id,
             |page| {
            let num_slots = page.get_num_slots()?;
            if page.capasity() == num_slots {
                // search free page
                for i in 0..num_slots {
                    let slot_no = SlotNo::new(i);
                    if page.is_free_slot(slot_no)? {
                        // found free slot
                        page.set_slot(slot_no, data)?;
                        return Ok(Some(RecordId::new(page_id, slot_no)))
                    }
                }
                return Ok(None)
            } else {
                // add new page
                let slot_no = page.add_slot(data)?;
                return Ok(Some(RecordId::new(page_id, slot_no)))
            }
        })?;

        match success {
            Some(rid) => Ok(rid),
            None => {
                // explore next page
                let next_page_id = self.with_record_page(page_id,
                    |page| 
                    page.get_next_page_id())?;
                if next_page_id > 1 {
                    // next page found
                    self.insert_record_page(next_page_id, data)
                } else {
                    // next page isn't exists. create new one
                    let new_page_id = create_page(&self.bufmgr)?;
                    self.with_record_page(page_id, |page| {
                        page.set_next_page_id(new_page_id)
                    })?;
                    self.with_record_page(new_page_id, |new_page| {
                        new_page.set_prev_page_id(page_id)
                    })?;
                    Ok(RecordId::new(new_page_id, SlotNo::new(0)))
                }
            }
        }
    }

    pub fn get_record(&self, rid: RecordId) -> Res<[u8; PAGE_RECORD_BYTE]> {
        self.with_record_page(rid.page_id, |page| {
            let data = page.get_slot(rid.slot_no)?;
            Ok(data)
        })
    }

    pub fn delete_record(&mut self, rid: RecordId) -> Res<()> {
        self.with_record_page(rid.page_id, |page| {
            page.set_slot_bit(rid.slot_no, 0)?;
            Ok(())
        })
    }
}

pub struct HeapFileScan {
    heap_file: Arc<Mutex<HeapFile>>,
    status: ScanStatus,
}

enum ScanStatus {
    Starting,
    Scanning(RecordId),
    Finished
}

impl HeapFileScan {
    pub fn new(heap_file: Arc<Mutex<HeapFile>>) -> HeapFileScan {
        HeapFileScan {
            heap_file,
            status: ScanStatus::Starting,
        }
    }

    pub fn with_record_page<F, T>(&self, page_id: PageId, f: F) -> Res<T> 
    where F: FnOnce(&mut RecordPage) -> Res<T> {
        let hf = self.heap_file.lock().unwrap();
        with_record_page(f, page_id, &hf.bufmgr)
    }

    pub fn get_next(&mut self) -> Res<Option<[u8; PAGE_RECORD_BYTE]>> {
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
                Ok(Some(rec))
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
