use crate::converter::set_int_value;
use crate::types::{Res, Error};

use super::AttributeType;
use super::schema::Schema;
use super::super::filemgr::PAGE_RECORD_BYTE;
use super::super::converter::get_int_value;

pub struct Record<'a> {
    // FIXME: use reference
    data: [u8; PAGE_RECORD_BYTE],
    schema: &'a Schema,
}

impl<'a> Record<'a> {
    pub fn new(data: [u8; PAGE_RECORD_BYTE], schema: &Schema) -> Record {
        Record {
            data,
            schema,
        }        
    }

    pub fn new_zero(schema: &Schema) -> Record {
        let data = [0; PAGE_RECORD_BYTE];
        Self::new(data, schema)
    }
    // pub fn get_byte(&self, pos: usize) -> Option<&u8> {
    //     self.data.get(pos)
    // }

    pub fn get_field_len(&self) -> usize {
        self.schema.len()
    }

    pub fn set_int_field(&mut self, fno: usize, v: i32) -> Res<()> {
        match self.schema.get_type(fno) {
            Some(AttributeType::Int) => {
                let offset = *self.schema.get_offset(fno).unwrap();
                set_int_value(&mut self.data, offset, v);
                Ok(())
            }
            _ => Err(Error::InvalidArg { 
                    msg: format!("Record::set_int_field: field is not int. fno={}", fno) })
        }
    }

    pub fn get_int_field(&self, fno: usize) -> Option<i32> {
        match self.schema.get_type(fno) {
            Some(AttributeType::Int) => {
                let offset = self.schema.get_offset(fno)?;
                let v = get_int_value(&self.data, *offset)?;
                Some(v)
            }
            _ => None
        }
    }

    pub fn set_varchar_field(&mut self ,fno: usize, v: &String) -> Res<()> {
        match self.schema.get_type(fno) {
            Some(AttributeType::Varchar(n)) => {
                let offset = *self.schema.get_offset(fno).unwrap();
                let xs = v.as_bytes();
                self.data[offset..offset+n].copy_from_slice(xs);
                Ok(())
            }
            _ => Err(Error::InvalidArg { 
                    msg: format!("Record::set_int_field: field is not int. fno={}", fno) })
        }
    }

    pub fn get_varchar_field(&self, fno: usize) -> Option<String> {
        match self.schema.get_type(fno) {
            Some(AttributeType::Varchar(n)) => {
                let offset = *self.schema.get_offset(fno)?;
                let xs = &self.data[offset..offset+n];
                let vec = Vec::from(xs);
                let a = String::from_utf8(vec).unwrap();
                Some(a)
            }
            _ => None
        }
    }

    pub fn get_as_string(&self, fno: usize) -> Option<String> {
        let ty = self.schema.get_type(fno)?;
        match ty {
            AttributeType::Int => {
                let x = self.get_int_field(fno)?;
                Some(format!("{}", x))
            }
            AttributeType::Varchar(_) => {
                self.get_varchar_field(fno)
            }
        }        
    }

    pub fn get_data(&self) -> &[u8; PAGE_RECORD_BYTE] {
        &self.data
    }
}

impl<'a> std::fmt::Display for Record<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for fno in 0..self.schema.len() {
            if fno > 0 {
                write!(f, ",  ")?;
            }
            let name = self.schema.get_name(fno).unwrap();
            let ty = self.schema.get_type(fno).unwrap();
            let v = self.get_as_string(fno).unwrap();
            write!(f, "{}({}): {}", name, ty, v)?
        }
        Ok(())
    }
}