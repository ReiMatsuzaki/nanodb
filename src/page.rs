use super::types::{Res, Error};
pub const PAGE_BYTE: usize = 64; // each page has 64 bytes

pub struct Page {
    data: [u8; PAGE_BYTE],
}

impl Page {
    pub fn new() -> Self {
        Self {
            data: [0; PAGE_BYTE],
        }
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn get_data_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    // pub fn set_data(&mut self, data: &[u8]) -> Res<()> {
    //     if data.len() != PAGE_BYTE {
    //         return Err(Error::InvalidArg{ msg: format!("data length must be {}", PAGE_BYTE)});
    //     }
    //     self.data.copy_from_slice(data);
    //     Ok(())
    // }

    pub fn get_int_value(&self, offset: usize) -> Res<i32> {
        if offset + 4 > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        let mut buf = [0; 4];
        buf.copy_from_slice(&self.data[offset..offset+4]);
        Ok(i32::from_le_bytes(buf))
    }

    pub fn set_int_value(&mut self, offset: usize, value: i32) -> Res<()> {
        if offset + 4 > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        self.data[offset..offset+4].copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    pub fn get_varchar_value(&self, offset: usize, length: usize) -> Res<String> {
        if offset + length > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        let mut buf = vec![0; length];
        buf.copy_from_slice(&self.data[offset..offset+length]);
        Ok(String::from_utf8(buf).unwrap())
    }

    pub fn set_varchar_value(&mut self, offset: usize, value: &str) -> Res<()> {
        if offset + value.len() > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        self.data[offset..offset+value.len()].copy_from_slice(value.as_bytes());
        Ok(())
    }

    pub fn get_byte_value(&self, offset: usize) -> Res<u8> {
        if offset + 1 > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        Ok(self.data[offset])
    }

    pub fn set_byte_value(&mut self, offset: usize, value: u8) -> Res<()> {
        if offset + 1 > PAGE_BYTE {
            return Err(Error::InvalidArg{ msg: format!("offset must be less than {}", PAGE_BYTE)});
        }
        self.data[offset] = value;
        Ok(())
    }
}