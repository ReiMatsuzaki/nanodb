pub mod heap_file;
pub mod hfilemgr;

pub use heap_file::*;
pub use hfilemgr::*;

use crate::page::PAGE_BYTE;

use super::types::{Res, PageId, EntryNo, Error};
use super::page::Page;
use super::diskmgr::DiskMgr;
use super::bufmgr::BufMgr;

const HEADER_START_FILE_ENTRY: usize = 10;
const HEADER_NAME_BYTE: usize = 20;
const HEADER_FILE_ENTRY_BYTE: usize = 4 + 4 + HEADER_NAME_BYTE + 2;

pub const PAGE_RECORD_BYTE: usize = 20;
const PAGE_NEXT_PAGE_ID: usize = 0;
const PAGE_PREV_PAGE_ID: usize = 4;
const PAGE_RECORD_START: usize = 10;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct SlotNo {
    pub value: usize,
}
impl SlotNo {
    pub fn new(value: usize) -> Self {
        Self { value }
    }
}
#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
pub struct RecordId {
    pub page_id: PageId,
    pub slot_no: SlotNo,
}

impl std::fmt::Display for RecordId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({}, {})", self.page_id, self.slot_no.value)
    }
}

impl RecordId {
    pub fn new(page_id: PageId, slot_no: SlotNo) -> RecordId {
        RecordId {
            page_id,
            slot_no,
        }
    }
}

pub struct FileMgr {
    bufmgr: BufMgr,
    header_page_id: PageId,
}

impl FileMgr {
    pub fn build(bufmgr: BufMgr) -> Res<Self> {
        let mut bufmgr = bufmgr;
        let (header_page_id, _) = bufmgr.create_page()?;
        let mgr = Self {
            bufmgr,
            header_page_id,
        };
        Ok(mgr)
    }

    fn pin_header_page(&mut self) -> Res<HeaderPage> {
        let header_page = self.bufmgr.pin_page(self.header_page_id)?;
        Ok(HeaderPage::new(header_page))
    }

    fn pin_record_page(&mut self, page_id: PageId) -> Res<RecordPage> {
        let page = self.bufmgr.pin_page(page_id)?;
        Ok(RecordPage::new(page))
    }

    pub fn create_file(&mut self, name: &str) -> Res<EntryNo> {
        let (page_id, _) = self.bufmgr.create_page()?;

        let mut header_page = self.pin_header_page()?;
        let entry_no = header_page.new_entry()?;
        header_page.set_head_free_page_id(entry_no, page_id)?;
        header_page.set_head_full_page_id(entry_no, 0)?;
        header_page.set_name(entry_no, name)?;
        self.bufmgr.unpin_page(self.header_page_id)?;

        // FIXME: summarize as new page procedure
        let hpid = self.header_page_id;
        let mut first_page = self.pin_record_page(page_id)?;
        first_page.set_next_page_id(0)?; // set invalid page id
        first_page.set_prev_page_id(hpid)?;

        Ok(entry_no)
    }

    pub fn insert_record(&mut self, entry_no: EntryNo, data: [u8; PAGE_RECORD_BYTE]) -> Res<()> {
        let page_id = self.first_page_id(entry_no)?;
        self.insert_record_in_page(page_id, data)
    }

    fn insert_record_in_page(&mut self, page_id: PageId, data: [u8; PAGE_RECORD_BYTE]) -> Res<()> {
        if page_id == 0 {
            return Err(Error::NoFreePage);
        }

        let mut page = self.pin_record_page(page_id)?;
        let num_slots = page.get_num_slots()?;
        // println!("capacity={}, num_slots={}", page.capasity(), num_slots);
        if page.capasity() == num_slots {
            // search free-page
            for i in 0..num_slots {
                let slot_no = SlotNo::new(i);
                if page.is_free_slot(slot_no)? {
                    // found free slot
                    page.set_slot(slot_no, data)?;
                    self.bufmgr.unpin_page(page_id)?;
                    // self.bufmgr.flush_page(page_id)?;
                    return Ok(())
                }
            }
        } else {
            // enough capasity, add new record
            page.add_slot(data)?;
            self.bufmgr.unpin_page(page_id)?;
            return Ok(())
        }

        // explore next page
        let next_page_id = page.get_next_page_id()?;
        self.bufmgr.unpin_page(page_id)?;

        if next_page_id > 1 {
            // valid record page id
            self.insert_record_in_page(next_page_id, data)
        } else {
            // next page is null, so create new page.
            let (new_page_id, _) = self.bufmgr.create_page()?;

            let mut page = self.pin_record_page(page_id)?;
            page.set_next_page_id(new_page_id)?;
            self.bufmgr.unpin_page(page_id)?;

            let mut new_page = self.pin_record_page(new_page_id)?;
            new_page.set_prev_page_id(page_id)?;
            self.bufmgr.unpin_page(new_page_id)?;

            self.insert_record_in_page(new_page_id, data)
        }


        // FIXME: move full page
    }

    pub fn get_record(&mut self, rid: RecordId) -> Res<[u8; PAGE_RECORD_BYTE]> {
        let mut page = self.pin_record_page(rid.page_id)?;
        let data = page.get_slot(rid.slot_no)?;
        self.bufmgr.unpin_page(rid.page_id)?;
        Ok(data)
    }

    fn first_page_id(&mut self, entry_no: EntryNo) -> Res<PageId> {
        let mut header_page = self.pin_header_page()?;
        let page_id = header_page.get_head_free_page_id(entry_no)?;
        self.bufmgr.unpin_page(self.header_page_id)?;
        Ok(page_id)
    }

    pub fn print_file(&mut self, entry_no: EntryNo) -> Res<()> {
        let mut rid = self.init_rid(entry_no)?;
        while let Some(next_rid) = rid {
            let rec = self.get_record(next_rid)?;
            println!("{:}:{:} {:?}", next_rid.page_id, next_rid.slot_no.value, rec);
            rid = self.next_rid(next_rid)?;
        }
        Ok(())
    }

    pub fn delete_record(&mut self, rid: RecordId) -> Res<()> {
        let mut page = self.pin_record_page(rid.page_id)?;
        page.set_slot_bit(rid.slot_no, 0)?;
        self.bufmgr.unpin_page(rid.page_id)?;
        Ok(())
    }

    pub fn init_rid(&mut self, eno: EntryNo) -> Res<Option<RecordId>> {
        let page_id = self.first_page_id(eno)?;
        self.next(page_id, None)
    }

    pub fn next_rid(&mut self, rid: RecordId) -> Res<Option<RecordId>> {
        self.next(rid.page_id, Some(rid.slot_no))
    }

    fn next(&mut self, pid: PageId, slot_no: Option<SlotNo>) -> Res<Option<RecordId>> {
        let mut page = self.pin_record_page(pid)?;
        let num_slots = page.get_num_slots()?;
        let mut slot_no = match slot_no {
            Some(slot_no) => slot_no.value + 1,
            None => 0,
        };
        while slot_no < num_slots {
            if !page.is_free_slot(SlotNo::new(slot_no))? {
                return Ok(Some(RecordId {
                    page_id: pid,
                    slot_no: SlotNo::new(slot_no),
                }));
            }
            slot_no += 1;
        }

        let next_page_id = page.get_next_page_id()?;
        if next_page_id == 0 {
            return Ok(None);
        }
        self.next(next_page_id, None)
    }
}

pub struct HeaderPage<'a> { page: &'a mut Page }

impl<'a> HeaderPage<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Self { page }
    }

    fn new_entry(&mut self) -> Res<EntryNo> {
        let mut eno = 0;
        while eno < 10 {
            let page_id = self.get_head_free_page_id(EntryNo::new(eno))?;
            if page_id == 0 {
                return Ok(EntryNo::new(eno));
            }
            eno += 1;
        }
        Err(Error::InvalidArg { msg: "FileMgr::create_file: too much entry".to_string() })
    }

    fn pos_head_free_page_id(&self, entry_no: EntryNo) -> usize {
        HEADER_START_FILE_ENTRY + entry_no.value * HEADER_FILE_ENTRY_BYTE        
    }

    pub fn set_head_free_page_id(&mut self, entry_no: EntryNo, page_id: PageId) -> Res<()> {
        let position = self.pos_head_free_page_id(entry_no);
        self.page.set_int_value(position, page_id as i32)?; // for free-page
        Ok(())
    }

    pub fn get_head_free_page_id(&mut self, entry_no: EntryNo) -> Res<PageId> {
        let position = self.pos_head_free_page_id(entry_no);
        let page_id = self.page.get_int_value(position)? as PageId;
        Ok(page_id)
    }

    fn pos_head_full_page_id(&self, entry_no: EntryNo) -> usize {
        self.pos_head_free_page_id(entry_no) + 4
    }

    pub fn set_head_full_page_id(&mut self, entry_no: EntryNo, page_id: PageId) -> Res<()> {
        let position = self.pos_head_full_page_id(entry_no);
        self.page.set_int_value(position, page_id as i32)?; // for free-page
        Ok(())
    }

    // pub fn get_head_full_page_id(&mut self, entry_no: EntryNo) -> Res<PageId> {
    //     let position = self.pos_head_full_page_id(entry_no);
    //     let page_id = self.page.get_int_value(position)? as PageId;
    //     Ok(page_id)
    // }    

    fn pos_name(&self, entry_no: EntryNo) -> usize {
        self.pos_head_free_page_id(entry_no) + 8
    }

    pub fn set_name(&mut self, entry_no: EntryNo, name: &str) -> Res<()> {
        if name.len() > HEADER_NAME_BYTE {
            return Err(Error::InvalidArg{ msg: format!("HeaderPage::set_name : name length must be less than {}", HEADER_NAME_BYTE)});
        }
        let position = self.pos_name(entry_no);
        self.page.set_varchar_value(position, &name)?;
        Ok(())
    }

    // pub fn get_name(&mut self, entry_no: EntryNo) -> Res<String> {
    //     let position = self.pos_name(entry_no);
    //     let name = self.page.get_varchar_value(position, 20)?;
    //     Ok(name)
    // }
}

pub struct RecordPage<'a> { page: &'a mut Page}

impl<'a> RecordPage<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Self { page }
    }

    pub fn set_num_slots(&mut self, num_slots: usize) -> Res<()> {
        self.page.set_int_value(PAGE_BYTE - 4, num_slots as i32)?;
        Ok(())
    }

    pub fn get_num_slots(&mut self) -> Res<usize> {
        let num_slots = self.page.get_int_value(PAGE_BYTE - 4)? as usize;
        Ok(num_slots)
    }

    pub fn set_next_page_id(&mut self, page_id: PageId) -> Res<()> {
        self.page.set_int_value(PAGE_NEXT_PAGE_ID, page_id as i32)?;
        Ok(())
    }

    pub fn get_next_page_id(&mut self) -> Res<PageId> {
        let page_id = self.page.get_int_value(PAGE_NEXT_PAGE_ID)? as PageId;
        Ok(page_id)
    }

    pub fn set_prev_page_id(&mut self, page_id: PageId) -> Res<()> {
        self.page.set_int_value(PAGE_PREV_PAGE_ID, page_id as i32)?;
        Ok(())
    }

    // pub fn get_prev_page_id(&mut self) -> Res<PageId> {
    //     let page_id = self.page.get_int_value(PAGE_NEXT_PAGE_ID)? as PageId;
    //     Ok(page_id)
    // }

    pub fn capasity(&self) -> usize {
        // format
        // next_page_id: 4
        // prev_page_id: 4
        // slot_1: PAGE_RECORD_BYTE
        // ...
        // slot_n: PAGE_RECORD_BYTE
        // flag_1: 1,
        // ...
        // flag_n: 1,
        // num_slots: 4
        // total: 8 + PAGE_RECORD_BYTE * n + 1 * n + 4
        (PAGE_BYTE - 12) / (1 + PAGE_RECORD_BYTE)
    }

    pub fn set_slot_bit(&mut self, slot_no: SlotNo, bit: u8) -> Res<()> {
        let num_slots = self.get_num_slots()?;
        if slot_no.value >= num_slots {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::set_slot : slot_no must be less than {}", num_slots)});
        }
        let position = PAGE_BYTE - 5 - slot_no.value;
        self.page.set_byte_value(position, bit)?;
        Ok(())
    }

    pub fn is_free_slot(&mut self, slot_no: SlotNo) -> Res<bool> {
        let num_slots = self.get_num_slots()?;
        if slot_no.value >= num_slots {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::get_slot : slot_no must be less than {}", num_slots)});
        }
        let position = PAGE_BYTE - 5 - slot_no.value;
        let bit = self.page.get_byte_value(position)?;
        Ok(bit == 0)
    }

    fn pos_slot(&self, slot_no: SlotNo) -> usize {
        PAGE_RECORD_START + PAGE_RECORD_BYTE * slot_no.value
    }

    pub fn set_slot(&mut self, slot_no: SlotNo, data: [u8; PAGE_RECORD_BYTE]) -> Res<()> {
        let num_slots = self.get_num_slots()?;
        if slot_no.value >= num_slots {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::set_slot : slot_no must be less than {}", num_slots)});
        }
        let position = self.pos_slot(slot_no);
        for i in 0..PAGE_RECORD_BYTE {
            self.page.set_byte_value(position + i, data[i])?;
        }
        self.set_slot_bit(slot_no, 1)?;
        Ok(())
    }

    pub fn add_slot(&mut self, data: [u8; PAGE_RECORD_BYTE]) -> Res<SlotNo> {
        let num_slots = self.get_num_slots()?;
        if num_slots >= self.capasity() {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::add_slot : num_slots must be less than {}", self.capasity())});
        }
        let slot_no = SlotNo::new(num_slots);

        self.set_num_slots(num_slots + 1)?;
        self.set_slot(slot_no, data)?;
        self.set_slot_bit(slot_no, 1)?;
        Ok(slot_no)
    }

    pub fn get_slot(&mut self, slot_no: SlotNo) -> Res<[u8; PAGE_RECORD_BYTE]> {
        let num_slots = self.get_num_slots()?;
        if slot_no.value >= num_slots {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::get_slot : slot_no must be less than {}", num_slots)});
        }

        if self.is_free_slot(slot_no)? {
            return Err(Error::InvalidArg { 
                msg: format!(
                    "RecordPage::get_slot: try to get free slot. slot_no={}",
                    slot_no.value
                ) })
        }

        let position = self.pos_slot(slot_no);
        let mut data = [0; PAGE_RECORD_BYTE];
        for i in 0..PAGE_RECORD_BYTE {
            data[i] = self.page.get_byte_value(position + i)?;
        }
        Ok(data)
    }
}

pub fn run_filemgr() -> Res<()> {
    let name = "nano-filemgr.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let mut filemgr = FileMgr::build(bufmgr)?;

    let eno = filemgr.create_file("test")?;
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let num = 12;
    for i in 0..num {
        println!("insert {}", i);
        filemgr.insert_record(eno, data)?;
    }
    println!("print all");
    filemgr.print_file(eno)?;

    println!("delete slot_no=3");
    filemgr.delete_record(RecordId {page_id: 3, slot_no: SlotNo::new(3)} )?;

    println!("print all again");
    filemgr.print_file(eno)?;

    println!("insert new record");
    let data = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    filemgr.insert_record(eno, data)?;

    println!("print all again");
    filemgr.print_file(eno)?;

    let rec0 = filemgr.get_record(RecordId { page_id: 2, slot_no: SlotNo::new(0) })?;
    assert_eq!(rec0[2], 3);
    let rec1 = filemgr.get_record(RecordId { page_id: 3, slot_no: SlotNo::new(3) })?;
    assert_eq!(rec1[2], 13);
    std::fs::remove_file(name).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filemgr() {
        run_filemgr().unwrap();
    }
}