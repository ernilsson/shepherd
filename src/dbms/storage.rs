pub mod ephemeral;

use std::{
    fs::File,
    io::{self, Read, Seek, Write},
};

pub const PAGE_SIZE: usize = 8192;

pub fn read_page(file: &mut File, page: u64, buf: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    file.seek(io::SeekFrom::Start(page * PAGE_SIZE as u64))?;
    file.read(buf).map(|_| ())
}

pub fn write_page(file: &mut File, page: u64, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
    let max = file.metadata()?.len() / PAGE_SIZE as u64;
    if page > max {
        return Err(io::Error::other("tried to write distant page"));
    }
    file.seek(io::SeekFrom::Start(page * PAGE_SIZE as u64))?;
    file.write_all(buf).map(|_| ())
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::io::Write;

    use super::*;
    use crate::dbms::storage::{PAGE_SIZE, ephemeral};

    #[test]
    fn read_page_seeks_multiple_of_page_size() {
        ephemeral::file!(tmp {
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

    #[test]
    fn write_page_seeks_multiple_of_page_size() {
        ephemeral::file!(tmp {
            let write_buffer = [1u8; PAGE_SIZE];
            assert!(write_page(tmp.borrow_mut(), 0, &write_buffer).is_ok());

            let write_buffer = [2u8; PAGE_SIZE];
            assert!(write_page(tmp.borrow_mut(), 1, &write_buffer).is_ok());

            tmp.borrow_mut().seek(io::SeekFrom::Start(0)).unwrap();
            let mut read_buffer = [0u8; PAGE_SIZE * 2];
            tmp.borrow_mut().read_exact(&mut read_buffer).unwrap();
            assert_eq!(read_buffer[0..PAGE_SIZE], [1u8; PAGE_SIZE]);
            assert_eq!(read_buffer[PAGE_SIZE..PAGE_SIZE*2], [2u8; PAGE_SIZE]);
        });
    }

    #[test]
    fn write_page_given_distant_page() {
        ephemeral::file!(tmp {
            // Given *slightly* distant page.
            let write_buffer = [1u8; PAGE_SIZE];
            match write_page(tmp.borrow_mut(), 1, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
        }
        });
        ephemeral::file!(tmp {
            // Given *very* distant page.
            let write_buffer = [1u8; PAGE_SIZE];
            match write_page(tmp.borrow_mut(), 4, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
        }
        });
    }
}
