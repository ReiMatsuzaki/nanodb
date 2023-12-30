use crate::{filemgr::*, types::*};

use std::sync::{Arc, Mutex};

use super::*;
use crate::filemgr::{HeapFile, RawFileScan};

pub struct FileScan {
    raw_file_scan: RawFileScan,
    schema: Schema,
}

impl FileScan {
    pub fn new(heap_file: Arc<Mutex<HeapFile>>, schema: Schema) -> FileScan {
        let raw_file_scan = RawFileScan::new(heap_file);
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

    pub fn peer_next_rid(&mut self) -> Res<Option<RecordId>> {
        self.raw_file_scan.peer_next_rid()
    }

    pub fn get_next(&mut self) -> Res<Option<(RecordId, Record)>> {
        let res = self.raw_file_scan.get_next()?
        .map(|(rid, data)| {
            (rid, Record::new(data, &self.schema))
        });
        Ok(res)
    }

    pub fn print(heap_file: Arc<Mutex<HeapFile>>, schema: Schema) -> Res<()> {
        let mut a = FileScan::new(heap_file, schema);
        while let Some((rid, rec)) = a.get_next()? {
            println!("{}: {}", rid, rec);
        }
        Ok(())    
    }
}

// FIXME: generalize condition by closure
pub struct FileScanOnPage {
    base: FileScan,
    page_id: PageId,
}

impl FileScanOnPage {
    pub fn new(heap_file: Arc<Mutex<HeapFile>>, schema: Schema, page_id: PageId) -> FileScanOnPage {
        let base = FileScan::new(heap_file, schema);
        FileScanOnPage {
            base,
            page_id,
        }
    }

    pub fn peer_next_rid(&mut self) -> Res<Option<RecordId>> {
        while let Some(rid) = self.base.peer_next_rid()? {
            if rid.page_id == self.page_id {
                break;
            }
            self.base.get_next()?;
        }
        self.base.peer_next_rid()
        // let res = match self.base.peer_next_rid()? {
        //     None => None,
        //     Some(rid) if rid.page_id == self.page_id => Some(rid),
        //     Some(_rid) => {
        //         self.get_next()
        //     },
        // };
        // Ok(res)
    }

    pub fn get_next(&mut self) -> Res<Option<(RecordId, Record)>> {
        // self.base.get_next()
        while let Some(rid) = self.base.peer_next_rid()? {
            if rid.page_id == self.page_id {
                break;
            }
            self.base.get_next()?;
        }
        self.base.get_next()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::filemgr::HFileMgr;

    use super::*;

    #[test]
    fn test_merge_sort() -> Res<()> {
        let name = "nano-file-scan.db";
        let mut filemgr = HFileMgr::build_default(name)?;
        let mut file = filemgr.create_file("file0")?;
        let schema = Schema::build(vec![
            ("id".to_string(), AttributeType::Int),
            ("name".to_string(), AttributeType::Varchar(4)),
            ("score".to_string(), AttributeType::Int),
        ]);

        let scores = [1, 5, 2, 6, 7, 3, 8, 9, 2, 5, 1, 3, 9, 21, 10, 13];
        for i in 0..scores.len() {
            let mut rec = Record::new_zero(&schema);
            rec.set_int_field(0, i as i32)?;
            rec.set_varchar_field(1, &"KVM".to_string())?;
            rec.set_int_field(2, scores[i])?;
            file.insert_record(*rec.get_data())?;
        }

        let file = Arc::new(Mutex::new(file));
        println!("print all");
        FileScan::print(file.clone(), schema.clone())?;

        let mut scan = FileScanOnPage::new(file.clone(), schema.clone(), 3);
        println!("print only page 3");
        while let Some((rid, rec)) = scan.get_next()? {
            println!("{} : {}", rid, rec);
        }

        let mut scan = FileScanOnPage::new(file.clone(), schema.clone(), 3);
        scan.get_next()?;
        let (rid, _b) = scan.get_next()?.unwrap();
        assert_eq!(rid.page_id, 3);
        assert_eq!(rid.slot_no.value, 1);
        // assert_eq!(2, scan.peer_next_rid()?.unwrap().slot_no.value);
        // assert_eq!(2, scan.get_next()?.unwrap().0.slot_no.value);
        // while let Some(x) = scan.peer_next_rid()? {
        //     println!("{}", x);
        //     scan.get_next()?;
        // }

        std::fs::remove_file(name).unwrap();
        Ok(())
    }

}