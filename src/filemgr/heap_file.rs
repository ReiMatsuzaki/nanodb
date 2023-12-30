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
        let page_id = self.get_header_free_page_id()?;
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
                // add new slot
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
                    let slot_no = self.with_record_page(new_page_id, |new_page| {
                        new_page.set_prev_page_id(page_id)?;
                        new_page.add_slot(data)
                    })?;
                    Ok(RecordId::new(new_page_id, slot_no))
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

    pub fn get_header_free_page_id(&self) -> Res<PageId> {
        let page_id = self.with_header_page(|h| {
            h.get_head_free_page_id(self.entry_no)
        })?;
        Ok(page_id)
    }

}
