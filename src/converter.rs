pub fn set_int_value(data: &mut [u8], offset: usize, v: i32) {
    let xs = v.to_be_bytes();
    data[offset..offset+4].copy_from_slice(&xs)
}

pub fn get_int_value(data: &[u8], offset: usize) -> Option<i32> {
    let mut buf = [0; 4];
    buf.copy_from_slice(&data[offset..offset+4]);
    Some(i32::from_be_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_int() {
        let mut xs = [0; 20];
        let x = 9;
        set_int_value(&mut xs, 10, x);
        assert_eq!(x, get_int_value(&mut xs, 10).unwrap());
    }
}