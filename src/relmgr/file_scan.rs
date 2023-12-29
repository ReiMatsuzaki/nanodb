use crate::{filemgr::*, types::*};

use std::sync::{Arc, Mutex};

use super::*;
use crate::filemgr::{HeapFile, HeapFileScan};

pub struct FileScan {
    raw_file_scan: HeapFileScan,
    schema: Schema,
}

impl FileScan {
    pub fn new(heap_file: Arc<Mutex<HeapFile>>, schema: Schema) -> FileScan {
        let raw_file_scan = HeapFileScan::new(heap_file);
        FileScan {
            raw_file_scan,
            schema,
        }
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    } 

    pub fn get_field_len(&self) -> usize {
        self.schema.len()
    }

    pub fn get_next(&mut self) -> Res<Option<(RecordId, Record)>> {
        let res = self.raw_file_scan.get_next()?
        .map(|(rid, data)| {
            (rid, Record::new(data, &self.schema))
        });
        Ok(res)
    }
}
