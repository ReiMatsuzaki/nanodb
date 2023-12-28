pub fn get_int_value(data: &[u8], offset: usize) -> Option<i32> {
    let mut buf = [0; 4];
    buf.copy_from_slice(&data[offset..offset+4]);
    Some(i32::from_le_bytes(buf))
}
