use crate::{filemgr::*, types::*};

use super::*;

pub struct FileScan {
    // FIXME: use reference
    filemgr: FileMgr,
    schema: Schema,
    entry_no: EntryNo,
    rid: Option<RecordId>,
}

impl FileScan {
    pub fn build(schema: Schema, filemgr: FileMgr, entry_no: EntryNo) -> Res<Self> {
        let mut filemgr = filemgr;
        let rid = filemgr.init_rid(entry_no)?;
        Ok(Self {
            filemgr,
            schema,
            entry_no,
            rid,
        })
    }

    // pub fn has_next(&self) -> bool {
    //     self.rid.is_some()
    // }

    pub fn reset(&mut self) -> Res<()> {
        self.rid = self.filemgr.init_rid(self.entry_no)?;
        Ok(())
    }

    pub fn get_next(&mut self) -> Res<Option<Record>> {
        match self.rid {
            None => Ok(None),
            Some(rid) => {
                let data = self.filemgr.get_record(rid)?;
                let rec = Record::new(data, &self.schema);
                self.rid = self.filemgr.next_rid(rid)?;
                Ok(Some(rec))
            }
        }
    }
}   