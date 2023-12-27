use std::collections::HashMap;

use super::types::*;

// const RECORD_BYTES: usize = 8;
const SLOT_SIZE: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    values: Vec<RecordValue>,
}

impl Record {
    pub fn new(data: Vec<RecordValue>) -> Self {
        Self { values: data }
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn get(&self, pos: usize) -> Option<&RecordValue> {
        self.values.get(pos)
    }

    pub fn get_int(&self, pos: usize) -> Res<i32> {
        match self.values[pos] {
            RecordValue::Int(i) => Ok(i),
            _ => Err(Error::EmptyRecord),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordValue {
    Int(i32),
    Varchar(String),
}

#[derive(Clone)]
pub enum Slot {
    Record(Record),
    Empty,
}

pub struct Page {
    id: PageId,
    // record_size: usize,
    slots: Vec<Slot>,
}

impl Page {
    pub fn new(id: PageId) -> Self {
        let slots = vec![Slot::Empty; SLOT_SIZE];
        Self {
            id,
            // record_size: 0,
            slots,
        }
    }

    fn check_record_id(&self, record_id: &RecordId) -> Res<()> {
        if self.id != record_id.page_id {
            return Err(Error::PageMismatch)
        }
        Ok(())
    }

    pub fn get(&self, record_id: &RecordId) -> Res<&Record> {
        self.check_record_id(record_id)?;
        match self.slots.get(record_id.slot_no as usize) {
            Some(Slot::Record(record)) => Ok(record),
            _ => Err(Error::EmptyRecord),
        }
    }

    pub fn insert(&mut self, record: Record) -> Res<RecordId> {
        let slot_no = self.slots.len();
        self.slots.push(Slot::Record(record));
        Ok(RecordId::new(self.id, slot_no as u32))
    }

    pub fn delete(&mut self, record_id: &RecordId) -> Res<()> {
        self.check_record_id(record_id)?;
        self.slots[record_id.slot_no as usize] = Slot::Empty;
        Ok(())
    }
}

// FIXME: rename
pub struct BufferManager {
    // page_table: HashMap<(FileNo, PageId), Page>,
    page_table: HashMap<FileNo, Vec<Page>>,
    file_no_list: Vec<FileNo>,
}

impl BufferManager {
    pub fn new() -> Self {
        Self {
            page_table: HashMap::new(),
            file_no_list: Vec::new(),
        }
    }

    pub fn fetch_page(&mut self, file_no: FileNo, page_id: PageId) -> &mut Page {
        if self.page_table.contains_key(&file_no) {
            let pages = self.page_table.get_mut(&file_no).unwrap();
            if pages.len() > page_id.value as usize {
                return pages.get_mut(page_id.value as usize).unwrap();
            } else {
                let page = Page::new(page_id);
                pages.push(page);
                let page = pages.last_mut().unwrap();
                return page;
            }
        } else {
            self.page_table.insert(file_no, vec![Page::new(page_id)]);
            let pages = self.page_table.get_mut(&file_no).unwrap();
            let page = pages.last_mut().unwrap();
            return page;
        }
    }

    pub fn create_file(&mut self) -> FileNo {
        let file_no = FileNo::new(self.file_no_list.len() as u32);
        self.file_no_list.push(file_no);
        file_no
    }

    pub fn record_iterator(&self, file_no: FileNo) -> impl Iterator<Item = &Record> {
        self.page_table.get(&file_no)
            .into_iter()
            .flat_map(|pages| 
                pages.iter()
                .flat_map(|page| 
                    page.slots.iter()
                    .flat_map(|slot| 
                        match slot {
                            Slot::Record(record) => Some(record),
                            _ => None,
                        }
                )))
    }
    // pub fn insert_page(&mut self, file_no: FileNo, page: Page) -> Res<()> {
    //     let page_id = page.id;
    //     self.page_table.insert((file_no, page_id), page);
    //     Ok(())
    // }
}

pub fn run_buffer_manager() -> Res<()> {
    let mut buffer_manager = BufferManager::new();
    let page = buffer_manager.fetch_page(
        FileNo::new(1),
        PageId::new(1),
    );
    let data = vec![
        RecordValue::Int(1),
        RecordValue::Varchar("hello".to_string()),
    ];
    let rec = Record { values: data };
    let rid = page.insert(rec.clone())?;
    let rec2 = page.get(&rid)?;
    assert_eq!(rec.values, rec2.values);

    page.delete(&rid)?;
    let rec3 = page.get(&rid);
    assert!(rec3.is_err());
    Ok(())
}