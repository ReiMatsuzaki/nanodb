pub mod heap_file;
pub mod raw_file_scan;
pub mod hfilemgr;

pub use heap_file::*;
pub use raw_file_scan::*;
pub use hfilemgr::*;

use crate::page::PAGE_BYTE;

use super::types::{Res, PageId, EntryNo, Error};
use super::page::Page;
use super::diskmgr::DiskMgr;

const HEADER_START_FILE_ENTRY: usize = 10;
const HEADER_NAME_BYTE: usize = 20;
const HEADER_FILE_ENTRY_BYTE: usize = 4 + 4 + HEADER_NAME_BYTE + 2;

pub const PAGE_RECORD_BYTE: usize = 128;
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

const HEADER_MAX_ENTRY: usize = 10;

// FIXME: HeaderPage can be replaced as ordinary Relation table.
pub struct HeaderPage<'a> { page: &'a mut Page }

impl<'a> HeaderPage<'a> {
    pub fn new(page: &'a mut Page) -> Self {
        Self { page }
    }

    fn new_entry(&mut self) -> Res<EntryNo> {
        let mut eno = 0;
        while eno < HEADER_MAX_ENTRY {
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

    // FIXME: full_page_id is unnecessary
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

    pub fn get_name(&self, entry_no: EntryNo) -> Res<Option<String>> {
        let position = self.pos_name(entry_no);
        let a = self.page.get_varchar_value(position, HEADER_NAME_BYTE)?;
        Ok(Some(a))
    }

    pub fn find(&mut self, name: &str) -> Res<Option<EntryNo>> {
        if name.len() > HEADER_NAME_BYTE {
            return Err(Error::InvalidArg{ msg: format!("HeaderPage::find : name length must be less than {}", HEADER_NAME_BYTE)});
        }
        for eno in 0..HEADER_MAX_ENTRY {
            let entry_no = EntryNo::new(eno);
            if let Some(eno_name) = self.get_name(entry_no)? {
                let page_id = self.get_head_free_page_id(entry_no)?;
                if name == &eno_name[..name.len()] && page_id > 0 {
                    return Ok(Some(entry_no))
                }
            }
        }
        Ok(None)
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

    fn get_slot_bit(&mut self, slot_no: SlotNo) -> Res<u8> {
        self.check_slot_no(slot_no)?;
        let position = PAGE_BYTE - 5 - slot_no.value;
        self.page.get_byte_value(position)
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

    pub fn swap_slot(&mut self, i: SlotNo, j: SlotNo) -> Res<()> {
        log::debug!("RecordPage::swap_slot");
        self.check_slot_no(i)?;
        self.check_slot_no(j)?;

        let tmp_bit = self.get_slot_bit(j)?;
        let tmp_slot = self.get_slot(j)?;
        let i_bit = self.get_slot_bit(i)?;
        let i_slot = self.get_slot(i)?;
        self.set_slot_bit(j, i_bit)?;
        self.set_slot(j, i_slot)?;
        self.set_slot_bit(i, tmp_bit)?;
        self.set_slot(i, tmp_slot)?;

        Ok(())
    }

    fn check_slot_no(&mut self, slot_no: SlotNo) -> Res<()> {
        let num_slots = self.get_num_slots()?;
        if slot_no.value >= num_slots {
            return Err(Error::InvalidArg{ msg: format!("RecordPage::set_slot : slot_no must be less than {}", num_slots)});
        }
        Ok(())
    }

    pub fn free_all(&mut self) -> Res<()> {
        for slot_no in 0..self.capasity() {
            self.set_slot_bit(SlotNo::new(slot_no), 0)?
        }
        Ok(())
    }
}

