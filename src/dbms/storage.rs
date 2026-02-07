pub mod ethemeral;

use std::{
    fs::File,
    io::{self, Read, Seek},
};

pub const PAGE_SIZE: usize = 8192;

pub fn read_page(file: &mut File, page: u64, buf: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    file.seek(io::SeekFrom::Start(page * PAGE_SIZE as u64))?;
    file.read(buf).map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    use crate::dbms::storage::{PAGE_SIZE, ethemeral};

    #[test]
    fn read_page_seeks_multiple_of_page_size() {
        ethemeral::file!(tmp {
            let mut write_buffer = [0u8; PAGE_SIZE * 2];
            write_buffer[0..PAGE_SIZE].copy_from_slice(&[5u8; PAGE_SIZE]);
            write_buffer[PAGE_SIZE..PAGE_SIZE*2].copy_from_slice(&[9u8; PAGE_SIZE]);
            tmp.borrow_mut().write_all(&write_buffer).unwrap();

            let mut read_buffer = [0u8; PAGE_SIZE];
            read_page(tmp.borrow_mut(), 0, &mut read_buffer).unwrap();
            assert_eq!(read_buffer, [5u8; PAGE_SIZE]);
            read_page(tmp.borrow_mut(), 1, &mut read_buffer).unwrap();
            assert_eq!(read_buffer, [9u8; PAGE_SIZE]);
        });
    }
}
