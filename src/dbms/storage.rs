pub mod ephemeral;
mod integrity;

use std::{
    fs::File,
    io::{self, Read, Seek, Write},
};

pub const PAGE_SIZE: usize = 8192;
const CRC_POLY: u8 = 0xB0;

pub fn read_page(file: &mut File, page: u64, buf: &mut [u8; PAGE_SIZE]) -> io::Result<()> {
    let max = file.metadata()?.len() / PAGE_SIZE as u64;
    if page + 1 > max {
        return Err(io::Error::other("tried to read distant page"));
    }
    file.seek(io::SeekFrom::Start(page * PAGE_SIZE as u64))?;
    file.read_exact(buf).map(|_| ())
}

pub fn write_page(file: &mut File, page: u64, buf: &[u8; PAGE_SIZE]) -> io::Result<()> {
    let max = file.metadata()?.len() / PAGE_SIZE as u64;
    if page > max {
        return Err(io::Error::other("tried to write distant page"));
    }
    file.seek(io::SeekFrom::Start(page * PAGE_SIZE as u64))?;
    file.write_all(buf).map(|_| ())
}

pub fn copy_page(file: &mut File, src: u64, dst: u64) -> io::Result<()> {
    if src == dst {
        return Err(io::Error::other("tried to copy page to itself"));
    }
    let mut buf = [0u8; PAGE_SIZE];
    read_page(file, src, &mut buf)?;
    write_page(file, dst, &buf)
}

pub fn write_meta(file: &mut File, pair: (u64, u64), buf: &[u8; PAGE_SIZE - 1]) -> io::Result<()> {
    copy_page(file, pair.0, pair.1)?;
    // Ensure that the backup has reached the storage medium before continuing.
    file.sync_all()?;

    let mut page = [0u8; PAGE_SIZE];
    page[0..PAGE_SIZE - 1].copy_from_slice(buf);
    page[PAGE_SIZE - 1] = integrity::crc(CRC_POLY, buf);
    write_page(file, pair.0, &page)
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
    fn read_page_given_distant_page() {
        ephemeral::file!(tmp {
            let mut read_buffer = [0u8; PAGE_SIZE];
            match read_page(tmp.borrow_mut(), 0, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            let mut read_buffer = [0u8; PAGE_SIZE];
            match read_page(tmp.borrow_mut(), 1, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            let mut read_buffer = [0u8; PAGE_SIZE];
            match read_page(tmp.borrow_mut(), 4, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
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
            let write_buffer = [1u8; PAGE_SIZE];
            match write_page(tmp.borrow_mut(), 1, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            let write_buffer = [1u8; PAGE_SIZE];
            match write_page(tmp.borrow_mut(), 4, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
    }

    #[test]
    fn copy_page_given_invalid_page_combination() {
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            match copy_page(tmp.borrow_mut(), 0, 0) {
                Ok(_) => panic!("allowed copying page to itself"),
                Err(error) => assert_eq!("tried to copy page to itself", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            match copy_page(tmp.borrow_mut(), 1, 0) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            match copy_page(tmp.borrow_mut(), 4, 0) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            match copy_page(tmp.borrow_mut(), 0, 2) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[0u8; PAGE_SIZE]).unwrap();
            match copy_page(tmp.borrow_mut(), 0, 4) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
    }

    #[test]
    fn copy_page_copies_from_src_to_dst() {
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[1u8; PAGE_SIZE]).unwrap();
            write_page(tmp.borrow_mut(), 1, &[2u8; PAGE_SIZE]).unwrap();

            let mut buf = [0u8; PAGE_SIZE];
            read_page(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!([2u8; PAGE_SIZE], buf);

            copy_page(tmp.borrow_mut(), 0, 1).unwrap();

            read_page(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!([1u8; PAGE_SIZE], buf);
        });
    }

    #[test]
    fn write_meta_when_backup_fails() {
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[1u8; PAGE_SIZE]).unwrap();
            // Making the backup page a distant page forces an error.
            match write_meta(tmp.borrow_mut(), (0, 2), &[0u8; PAGE_SIZE-1]) {
                Ok(_) => panic!("allowed backup page failure"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
            let mut buf = [0u8; PAGE_SIZE];
            read_page(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; PAGE_SIZE], buf);
        });
    }

    #[test]
    fn write_meta_when_main_fails() {
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[1u8; PAGE_SIZE]).unwrap();
            // Making the main page a distant page forces an error.
            match write_meta(tmp.borrow_mut(), (2, 0), &[0u8; PAGE_SIZE-1]) {
                Ok(_) => panic!("allowed main page failure"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
            let mut buf = [0u8; PAGE_SIZE];
            read_page(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; PAGE_SIZE], buf);
        });
    }

    #[test]
    fn write_meta_without_errors() {
        ephemeral::file!(tmp {
            write_page(tmp.borrow_mut(), 0, &[1u8; PAGE_SIZE]).unwrap();
            write_page(tmp.borrow_mut(), 1, &[2u8; PAGE_SIZE]).unwrap();

            write_meta(tmp.borrow_mut(), (1, 0), &[3u8; PAGE_SIZE-1]).unwrap();

            let mut buf = [0u8; PAGE_SIZE];
            read_page(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([2u8; PAGE_SIZE], buf);

            read_page(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!(buf[0..PAGE_SIZE-1], [3u8; PAGE_SIZE-1]);
            assert_eq!(integrity::crc(CRC_POLY, &buf[0..PAGE_SIZE-1]), buf[PAGE_SIZE-1]);
        });
    }
}
