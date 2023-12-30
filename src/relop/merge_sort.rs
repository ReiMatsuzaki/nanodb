use std::sync::{Arc, Mutex};

use crate::types::*;
use crate::filemgr::HeapFile;
use super::Schema;

pub struct MergeSort<'a> {
    file: Arc<Mutex<HeapFile>>,
    schema: &'a Schema,
    sort_fno: usize,
}

impl<'a> MergeSort<'a> {
    pub fn new(file: Arc<Mutex<HeapFile>>, schema: &'a Schema, sort_fno: usize) -> MergeSort {
        MergeSort {
            file,
            schema,
            sort_fno
        }
    }

    pub fn sort(&mut self) -> Res<()> {
        todo!()
    }

    fn sort_page(&mut self) -> Res<()> {
        let hf = self.file.lock().unwrap();
        // hf.with_record_page(page_id, f)
        // hf.with_record_page()

        // self.file.with_record_page()
        todo!();
    }
}