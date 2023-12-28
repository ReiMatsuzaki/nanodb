use crate::page::PAGE_BYTE;

use super::types::{Res, PageId, EntryNo, Error};
use super::diskmgr::DiskMgr;
use super::bufmgr::BufMgr;

const HEADER_START_FILE_ENTRY: usize = 10;
const HEADER_FILE_ENTRY_BYTE: usize = 4 + 4 + 20 + 2;

const PAGE_RECORD_BYTE: usize = 20;
const PAGE_NEXT_PAGE_ID: usize = 0;
// const PAGE_PREV_PAGE_ID: usize = 4;
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

    pub fn create_file(&mut self, name: &str) -> Res<EntryNo> {
        let (page_id, _) = self.bufmgr.create_page()?;

        let entry_no = 0;
        let position = HEADER_START_FILE_ENTRY + entry_no * HEADER_FILE_ENTRY_BYTE;
        let header_page = self.bufmgr.pin_page(self.header_page_id)?;
        header_page.set_int_value(position, page_id as i32)?; // for free-page
        header_page.set_int_value(position+4, 0 as i32)?; // for full-page
        header_page.set_varchar_value(position + 8, name)?;
        self.bufmgr.unpin_page(self.header_page_id)?;
        Ok(EntryNo::new(entry_no))
    }

    pub fn insert_record(&mut self, entry_no: EntryNo, data: [u8; PAGE_RECORD_BYTE]) -> Res<()> {
        let position = HEADER_START_FILE_ENTRY + entry_no.value * HEADER_FILE_ENTRY_BYTE;
        let header_page = self.bufmgr.pin_page(self.header_page_id)?;
        let page_id = header_page.get_int_value(position+0)? as PageId;
        let page = self.bufmgr.pin_page(page_id)?;

        let num_slots = page.get_int_value(PAGE_BYTE - 4)? as usize;
        let position = PAGE_RECORD_START + PAGE_RECORD_BYTE * num_slots;

        if position + PAGE_RECORD_BYTE > PAGE_BYTE - 8 - num_slots {
            // search free-page
            for i in 0..num_slots {
                let bit = page.get_byte_value(PAGE_BYTE - 8 - i)?;
                if bit == 0 { // found free space
                    let position = PAGE_RECORD_START + PAGE_RECORD_BYTE * i;
                    for j in 0..PAGE_RECORD_BYTE {
                        page.set_byte_value(position + j, data[j])?;
                    }
                    page.set_byte_value(PAGE_BYTE-8-i, 1)?;
                    self.bufmgr.unpin_page(page_id)?;
                    self.bufmgr.flush_page(page_id)?;
                    return Ok(())
                }
            }
        } else {
            // insert new record
            for j in 0..PAGE_RECORD_BYTE {
                page.set_byte_value(position + j, data[j])?;
            }
            page.set_byte_value(PAGE_BYTE-8-num_slots, 1)?;
            page.set_int_value(PAGE_BYTE - 4, num_slots as i32 + 1)?;
            self.bufmgr.unpin_page(page_id)?;
            self.bufmgr.flush_page(page_id)?;
            return Ok(())
        }
        Err(Error::NoFreePage)
    }

    pub fn get_record(&mut self, rid: RecordId) -> Res<[u8; PAGE_RECORD_BYTE]> {
        let page = self.bufmgr.pin_page(rid.page_id)?;
        let position = PAGE_RECORD_START + PAGE_RECORD_BYTE * rid.slot_no.value;
        let mut data = [0; PAGE_RECORD_BYTE];
        for i in 0..PAGE_RECORD_BYTE {
            data[i] = page.get_byte_value(position + i)?;
        }
        self.bufmgr.unpin_page(rid.page_id)?;
        Ok(data)
    }

    fn first_page_id(&mut self, entry_no: EntryNo) -> Res<PageId> {
        let position = HEADER_START_FILE_ENTRY + entry_no.value * HEADER_FILE_ENTRY_BYTE;
        let header_page = self.bufmgr.pin_page(self.header_page_id)?;
        let page_id = header_page.get_int_value(position+0)? as PageId;
        self.bufmgr.unpin_page(self.header_page_id)?;
        Ok(page_id)
        // let page = self.bufmgr.pin_page(page_id)?;
        // Ok(page)
    }

    pub fn print_file(&mut self, entry_no: EntryNo) -> Res<()> {
        let mut page_id = self.first_page_id(entry_no)?;
        while page_id != 0 {
            let page = self.bufmgr.pin_page(page_id)?;
            let num_slots = page.get_int_value(PAGE_BYTE - 4)? as usize;
            for i in 0..num_slots {
                let bit = page.get_byte_value(PAGE_BYTE - 8 - i)?;
                if bit == 1 { // occupied
                    let position = PAGE_RECORD_START + PAGE_RECORD_BYTE * i;
                    print!("{}:{}: ", page_id, i);
                    for j in 0..PAGE_RECORD_BYTE {
                        let x = page.get_byte_value(position + j)?;
                        print!("{} ", x);
                    }
                    println!("");
                }
            }
            page_id = page.get_int_value(PAGE_NEXT_PAGE_ID)? as PageId;
        }
        Ok(())
    }

    pub fn delete_record(&mut self, page_id: PageId, slot_no: usize) -> Res<()> {
        let page = self.bufmgr.pin_page(page_id)?;
        // let num_slots = page.get_int_value(PAGE_BYTE - 4)? as usize;
        let position = PAGE_RECORD_START + PAGE_RECORD_BYTE * slot_no;
        for i in 0..PAGE_RECORD_BYTE {
            page.set_byte_value(position + i, 0)?;
        }
        page.set_byte_value(PAGE_BYTE-8-slot_no, 0)?;
        // page.set_int_value(PAGE_BYTE - 4, num_slots as i32 - 1)?;
        self.bufmgr.unpin_page(page_id)?;
        self.bufmgr.flush_page(page_id)?;
        Ok(())
    }
    // pub fn destroy_file(&mut self, entry_no: EntryNo) -> Res<()> {
    //     let start_file_entry = 10;
    //     let file_entry_bytes = 8 + 20 + 2;
    //     let position = START_FILE_ENTRY + entry_no.value * file_entry_bytes;
    //     let mut header_page = self.bufmgr.pin_page(self.header_page_id)?;
    //     let page_id = header_page.get_int_value(position)? as PageId;
    //     self.bufmgr.free_page(page_id)?;
    //     header_page.set_int_value(position, -1 as i32)?;
    //     header_page.set_int_value(position+4, -1 as i32)?;
    //     header_page.set_varchar_value(position + 8, "")?;
    //     Ok(())
    // }

    pub fn init_rid(&mut self, eno: EntryNo) -> Res<Option<RecordId>> {
        let page_id = self.first_page_id(eno)?;
        self.next(page_id, None)
    }

    pub fn next_rid(&mut self, rid: RecordId) -> Res<Option<RecordId>> {
        self.next(rid.page_id, Some(rid.slot_no))
    }

    fn next(&mut self, pid: PageId, slot_no: Option<SlotNo>) -> Res<Option<RecordId>> {
        let page = self.bufmgr.pin_page(pid)?;
        let num_slots = page.get_int_value(PAGE_BYTE - 4)? as usize;
        let mut slot_no = match slot_no {
            Some(slot_no) => slot_no.value + 1,
            None => 0,
        };
        while slot_no < num_slots {
            let bit = page.get_byte_value(PAGE_BYTE - 8 - slot_no)?;
            if bit == 1 {
                return Ok(Some(RecordId {
                    page_id: pid,
                    slot_no: SlotNo::new(slot_no),
                }));
            }
            slot_no += 1;
        }
        let next_page_id = page.get_int_value(PAGE_NEXT_PAGE_ID)? as PageId;
        if next_page_id == 0 {
            return Ok(None);
        }
        let next_page = self.bufmgr.pin_page(next_page_id)?;
        let bit = next_page.get_byte_value(PAGE_BYTE - 8)?;
        if bit == 1 {
            return Ok(Some(RecordId {
                page_id: next_page_id,
                slot_no: SlotNo::new(0),
            }));
        }
        Ok(None)
    }
}

pub fn run_filemgr() -> Res<()> {
    let name = "nano-filemgr.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let mut filemgr = FileMgr::build(bufmgr)?;

    let eno = filemgr.create_file("test")?;
    let data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    let num = 5;
    for i in 0..num {
        println!("insert {}", i);
        filemgr.insert_record(eno, data)?;
    }
    println!("print all");
    filemgr.print_file(eno)?;

    println!("delete slot_no=3");
    filemgr.delete_record(2, 3)?;

    println!("print all again");
    let mut rid = filemgr.init_rid(eno)?;
    while let Some(next_rid) = rid {
        let rec = filemgr.get_record(next_rid)?;
        println!("{:}:{:} {:?}", next_rid.page_id, next_rid.slot_no.value, rec);
        rid = filemgr.next_rid(next_rid)?;

    }

    println!("insert new record");
    let data = [11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
    filemgr.insert_record(eno, data)?;

    println!("print all again");
    filemgr.print_file(eno)?;

    let mut rid = filemgr.init_rid(eno)?;
    while let Some(next_rid) = rid {
        println!("next_rid: {:?}", next_rid);
        rid = filemgr.next_rid(next_rid)?;
    }

    let rec0 = filemgr.get_record(RecordId { page_id: 2, slot_no: SlotNo::new(0) })?;
    assert_eq!(rec0[2], 3);
    let rec1 = filemgr.get_record(RecordId { page_id: 2, slot_no: SlotNo::new(3) })?;
    assert_eq!(rec1[2], 13);

    std::fs::remove_file(name).unwrap();
    Ok(())
}