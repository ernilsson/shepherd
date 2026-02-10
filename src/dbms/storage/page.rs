use std::{
    fs::File,
    io::{self, Read, Seek, Write},
};

pub const SIZE: usize = 8192;

pub fn read(file: &mut File, page: u64, buf: &mut [u8; SIZE]) -> io::Result<()> {
    let max = file.metadata()?.len() / SIZE as u64;
    if page + 1 > max {
        return Err(io::Error::other("tried to read distant page"));
    }
    file.seek(io::SeekFrom::Start(page * SIZE as u64))?;
    file.read_exact(buf).map(|_| ())
}

pub fn write(file: &mut File, page: u64, buf: &[u8; SIZE]) -> io::Result<()> {
    let max = file.metadata()?.len() / SIZE as u64;
    if page > max {
        return Err(io::Error::other("tried to write distant page"));
    }
    file.seek(io::SeekFrom::Start(page * SIZE as u64))?;
    file.write_all(buf).map(|_| ())
}

pub fn copy(file: &mut File, src: u64, dst: u64) -> io::Result<()> {
    if src == dst {
        return Err(io::Error::other("tried to copy page to itself"));
    }
    let mut buf = [0u8; SIZE];
    read(file, src, &mut buf)?;
    write(file, dst, &buf)
}

#[cfg(test)]
mod tests {
    use crate::dbms::storage::ephemeral;

    use super::*;
    use std::io::Write;

    #[test]
    fn read_seeks_multiple_of_page_size() {
        ephemeral::file!(tmp {
            let mut write_buffer = [0u8; SIZE * 2];
            write_buffer[0..SIZE].copy_from_slice(&[5u8; SIZE]);
            write_buffer[SIZE..SIZE*2].copy_from_slice(&[9u8; SIZE]);
            tmp.borrow_mut().write_all(&write_buffer).unwrap();

            let mut read_buffer = [0u8; SIZE];
            read(tmp.borrow_mut(), 0, &mut read_buffer).unwrap();
            assert_eq!(read_buffer, [5u8; SIZE]);
            read(tmp.borrow_mut(), 1, &mut read_buffer).unwrap();
            assert_eq!(read_buffer, [9u8; SIZE]);
        });
    }

    #[test]
    fn read_given_distant_page() {
        ephemeral::file!(tmp {
            let mut read_buffer = [0u8; SIZE];
            match read(tmp.borrow_mut(), 0, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            let mut read_buffer = [0u8; SIZE];
            match read(tmp.borrow_mut(), 1, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            let mut read_buffer = [0u8; SIZE];
            match read(tmp.borrow_mut(), 4, &mut read_buffer) {
                Ok(_) => panic!("allowed reading distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
    }

    #[test]
    fn write_seeks_multiple_of_page_size() {
        ephemeral::file!(tmp {
            let write_buffer = [1u8; SIZE];
            assert!(write(tmp.borrow_mut(), 0, &write_buffer).is_ok());

            let write_buffer = [2u8; SIZE];
            assert!(write(tmp.borrow_mut(), 1, &write_buffer).is_ok());

            tmp.borrow_mut().seek(io::SeekFrom::Start(0)).unwrap();
            let mut read_buffer = [0u8; SIZE * 2];
            tmp.borrow_mut().read_exact(&mut read_buffer).unwrap();
            assert_eq!(read_buffer[0..SIZE], [1u8; SIZE]);
            assert_eq!(read_buffer[SIZE..SIZE*2], [2u8; SIZE]);
        });
    }

    #[test]
    fn write_given_distant_page() {
        ephemeral::file!(tmp {
            let write_buffer = [1u8; SIZE];
            match write(tmp.borrow_mut(), 1, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            let write_buffer = [1u8; SIZE];
            match write(tmp.borrow_mut(), 4, &write_buffer) {
                Ok(_) => panic!("allowed writing distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
    }

    #[test]
    fn copy_given_invalid_page_combination() {
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            match copy(tmp.borrow_mut(), 0, 0) {
                Ok(_) => panic!("allowed copying page to itself"),
                Err(error) => assert_eq!("tried to copy page to itself", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            match copy(tmp.borrow_mut(), 1, 0) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            match copy(tmp.borrow_mut(), 4, 0) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            match copy(tmp.borrow_mut(), 0, 2) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[0u8; SIZE]).unwrap();
            match copy(tmp.borrow_mut(), 0, 4) {
                Ok(_) => panic!("allowed copying from distant page"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
        });
    }

    #[test]
    fn copy_copies_from_src_to_dst() {
        ephemeral::file!(tmp {
            write(tmp.borrow_mut(), 0, &[1u8; SIZE]).unwrap();
            write(tmp.borrow_mut(), 1, &[2u8; SIZE]).unwrap();

            let mut buf = [0u8; SIZE];
            read(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!([2u8; SIZE], buf);

            copy(tmp.borrow_mut(), 0, 1).unwrap();

            read(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!([1u8; SIZE], buf);
        });
    }
}
