use bytes::BytesMut;

#[derive(PartialEq, Eq)]
pub struct MutableByteBuffer {
    buf: BytesMut,
}

impl MutableByteBuffer {
    pub fn new(buf: &[u8], data_len: usize) -> Self {
        BytesMut::from(buf[0..data_len])
    }

    pub fn mutable_slice(&mut self, start: usize, end: usize) -> Result<&mut [u8]> {
        if start > self.buf.len() || end > self.buf.len() || end > start {
            bail!("Invalid bounds for MutableByteBuffer");
        }
        Ok(&mut self.as_mut()[start..end])
    }
}
