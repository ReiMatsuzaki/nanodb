use std::sync::{Arc, Mutex};

use crate::{diskmgr::DiskMgr, bufmgr::BufMgr, filemgr::{HFileMgr, SlotNo, RecordId, RecordPage, PAGE_RECORD_BYTE}, relop::{AttributeType, Record, FileScan, FileScanOnPage}};
use crate::types::*;
use crate::filemgr::HeapFile;
use super::Schema;

pub struct Relation {
    file: Arc<Mutex<HeapFile>>,
    schema: Schema,    
}

impl Relation {
    pub fn new(file: Arc<Mutex<HeapFile>>, schema: Schema) -> Relation {
        Relation {
            file,
            schema
        }
    }

    pub fn get_record(&self, rid: RecordId) -> Res<Record> {
        let file = self.file.lock().unwrap();
        let data = file.get_record(rid)?;
        Ok(Record::new(data, &self.schema))
    }

    pub fn arc_mutex_file(&self) -> Arc<Mutex<HeapFile>> {
        self.file.clone()
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }

    pub fn truncate(&mut self) -> Res<()> {
        log::trace!("truncate");
        let f = self.file.lock().unwrap();
        f.with_record_pages(|_pid, p| {
            p.free_all()
        })?;
        Ok(())
    }

    // pub fn insert_record(&self, rec: Record) -> Res<()> {
    //     self.insert_bytes(*rec.get_data())
    // }

    pub fn insert_bytes(&self, data: [u8; PAGE_RECORD_BYTE]) -> Res<()> {
        let mut f = self.file.lock().unwrap();
        f.insert_record(data)?;
        Ok(())
    }
}

pub struct MergeSort {
    rel: Relation,
    key_fno: usize,
}

impl MergeSort {
    pub fn new(rel: Relation, key_fno: usize) -> MergeSort {
        MergeSort {
            rel,
            key_fno
        }
    }

    pub fn sort(&mut self) -> Res<()> {
        log::debug!("sort");
        // FIXME: build pids
        let mut pid = {
            let hf = self.rel.arc_mutex_file();
            let hf = hf.lock().unwrap();
            hf.get_header_free_page_id()?
        };
        let mut pids = Vec::new();
        while pid != 0 {
            pids.push(pid);
            let hf = self.rel.arc_mutex_file();
            let hf = hf.lock().unwrap();
            pid = hf.with_record_page(pid, |page| page.get_next_page_id())?;
        }

        self.pass_0()?;

        match pids.as_slice() {
            [_] => Ok(()),
            [pid0, pid1] => self.pass_1(*pid0, *pid1),
            _ => panic!("not implemented. merge_sort support at most two pages. num_pages={}", pids.len())
        }
    }

    fn pass_0(&mut self) -> Res<()> {
        // execute insertion sort for `pid` page
        // before each iteration (i),
        // xs[0..i] is sorted

        log::debug!("pass_0");
        let hf = self.rel.arc_mutex_file();
        let hf = hf.lock().unwrap();
        hf.with_record_pages(|_pid, page| {
            let num = page.get_num_slots()?;
            for i in 1..num {
                for j in (1..=i).rev() {
                    let jm1 = SlotNo::new(j-1);
                    let j = SlotNo::new(j);
                    // compare (j-1) and (j)
                    let xj = self.get_key(j, page)?;
                    let xjm1 = self.get_key(jm1, page)?;
                    if xjm1 > xj {
                        page.swap_slot(jm1, j)?;
                    }
                }
            }
            Ok(())
        })?;
        Ok(())
    }

    fn pass_1(&mut self, pid0: PageId, pid1: PageId) -> Res<()> {
        log::debug!("pass_1");
        let file = self.rel.arc_mutex_file();
        let schema = self.rel.get_schema();
        // let hf = file.lock().unwrap();
        let mut scan0 = FileScanOnPage::new(file.clone(), schema.clone(), pid0);
        let mut scan1 = FileScanOnPage::new(file.clone(), schema.clone(), pid1);

        // FIXME: use page
        let mut recs = Vec::new();

        log::trace!("pass_1: start merge sort loop");
        loop {
            let rec = match (scan0.peer_next_rid()?, scan1.peer_next_rid()?) {
                (Some(rid0), None) => {
                    scan0.get_next()?;
                    self.rel.get_record(rid0)?
                }
                (None, Some(rid1)) => {
                    scan1.get_next()?;
                    self.rel.get_record(rid1)?
                },
                (Some(rid0), Some(rid1)) => {
                    let rec0 = self.rel.get_record(rid0)?;
                    let rec1 = self.rel.get_record(rid1)?;
                    // log::debug!("{} : {}", rec0, rec1);
                    let key0 = rec0.get_int_field(self.key_fno).unwrap();
                    let key1 = rec1.get_int_field(self.key_fno).unwrap();
                    if key0 < key1 {
                        scan0.get_next()?;
                        rec0
                    } else {
                        scan1.get_next()?;
                        rec1
                    }
                },
                _ => break,
            };
            let d = rec.get_data();
            recs.push(*d);
        }

        log::trace!("pass_1: truncate existing data");
        self.rel.truncate()?;

        log::trace!("pass_1: insert sorted record");
        for data in recs {
            self.rel.insert_bytes(data)?
        }

        Ok(())
    }

    fn get_key(&self, slot_no: SlotNo, page: &mut RecordPage) -> Res<i32> {
        let a = page.get_slot(slot_no)?;
        let rec = Record::new(a, self.rel.get_schema());
        let v = rec.get_int_field(self.key_fno).unwrap();
        Ok(v)
    }

}

pub fn run_merge_sort() -> Res<()> {
    let name = "nano-merge-sort.db";
    let diskmgr = DiskMgr::open_db(name)?;
    let bufmgr = BufMgr::new(10, diskmgr);
    let bufmgr = Arc::new(Mutex::new(bufmgr));
    let mut filemgr = HFileMgr::build(bufmgr)?;

    let schema = Schema::build(vec![
        ("id".to_string(), AttributeType::Int),
        ("name".to_string(), AttributeType::Varchar(4)),
        ("score".to_string(), AttributeType::Int),
    ]);

    let mut file = filemgr.create_file("file0")?;

    let scores = [1, 5, 2, 6, 7, 3, 8, 9, 2, 5, 1, 3, 9];
    for i in 0..scores.len() {
        let mut rec = Record::new_zero(&schema);
        rec.set_int_field(0, i as i32)?;
        rec.set_varchar_field(1, &"KVM".to_string())?;
        rec.set_int_field(2, scores[i])?;
        file.insert_record(*rec.get_data())?;
    }

    let file = Arc::new(Mutex::new(file));

    // let mut a = FileScan::new(file.clone(), schema.clone());
    // while let Some((rid, rec)) = a.get_next()? {
    //     println!("{}: {}", rid, rec);
    // }
    log::info!("print before");
    FileScan::print(file.clone(), schema.clone())?;

    log::info!("merge sort");
    let rel = Relation::new(file.clone(), schema.clone());
    let mut merge_sort = MergeSort::new(rel, 2);
    merge_sort.sort()?;

    log::info!("print after");
    FileScan::print(file.clone(), schema.clone())?;

    std::fs::remove_file(name).unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_merge_sort() {
        run_merge_sort().unwrap()
    }

}