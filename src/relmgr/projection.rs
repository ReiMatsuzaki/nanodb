use crate::{types::*, filemgr::RecordId};
use super::{FileScan, Record, Schema, AttributeType};

pub struct Projection {
    // FIXME: define Iterator interface
    iterator: FileScan,
    schema: Schema,
    fnos: Vec<usize>,
}

impl Projection {
    pub fn build(iterator: FileScan, fnos: Vec<usize>) -> Res<Self> {
        let schema = iterator.get_schema()
        .projection(&fnos)
        .ok_or(Error::InvalidArg { 
            msg: format!(
                "there exist fno which exceed field size. field.len={}, fnos={:?}", 
                iterator.get_field_len(),
                fnos,
            )
        })?;
        Ok(Projection {
            iterator,
            schema,
            fnos,
        })
    }

    pub fn get_next(&mut self) -> Res<Option<(RecordId, Record)>> {
        let res = self.iterator.get_next()?;
        match res {
            None => Ok(None),
            Some((rid, rec)) => {
                let mut new_rec = Record::new_zero(&self.schema);
                for new_fno in 0..self.fnos.len() {
                    let fno = self.fnos.get(new_fno).unwrap();
                    // FIXME: transfer byte directory
                    match self.schema.get_type(new_fno).unwrap() {
                        AttributeType::Int => {
                            let v = rec.get_int_field(*fno).unwrap();
                            new_rec.set_int_field(new_fno, v)?;
                        }
                        AttributeType::Varchar(_) => {
                            let v = rec.get_varchar_field(*fno).unwrap();
                            new_rec.set_varchar_field(new_fno, &v)?;
                        }
                    }
                }
                Ok(Some((rid, new_rec)))
            }
        }
    }
}