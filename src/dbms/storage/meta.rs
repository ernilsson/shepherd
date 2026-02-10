use std::{fs::File, io};

use crate::dbms::storage::{integrity, page};

const CRC_POLY: u8 = 0xB0;

pub fn write(file: &mut File, pair: (u64, u64), buf: &[u8; page::SIZE - 1]) -> io::Result<()> {
    page::copy(file, pair.0, pair.1)?;
    // Ensure that the backup has reached the storage medium before continuing.
    file.sync_all()?;

    let mut page = [0u8; page::SIZE];
    page[0..page::SIZE - 1].copy_from_slice(buf);
    page[page::SIZE - 1] = integrity::crc(CRC_POLY, buf);
    page::write(file, pair.0, &page)
}

pub fn read(file: &mut File, pair: (u64, u64), buf: &mut [u8; page::SIZE - 1]) -> io::Result<()> {
    let mut page = [0u8; page::SIZE];
    page::read(file, pair.0, &mut page)?;
    if page[page::SIZE - 1] != integrity::crc(CRC_POLY, &page[0..page::SIZE - 1]) {
        // The calculated CRC is different from the stored CRC. It does not
        // matter what has gone wrong at this point, just that the backup data
        // should take the place of the main data.
        page::read(file, pair.1, &mut page)?;
        page[page::SIZE - 1] = integrity::crc(CRC_POLY, &page[0..page::SIZE - 1]);
        page::write(file, pair.0, &page)?;
    }
    buf.copy_from_slice(&page[0..page::SIZE - 1]);
    Ok(())
}

pub fn init(file: &mut File, pair: (u64, u64)) -> io::Result<()> {
    let mut page = [0u8; page::SIZE];
    page[page::SIZE - 1] = integrity::crc(CRC_POLY, &page[0..page::SIZE - 1]);
    page::write(file, pair.1, &page)?;
    file.sync_all()?;
    page::write(file, pair.0, &page)
}

#[cfg(test)]
mod tests {
    use core::panic;

    use super::*;
    use crate::dbms::storage::ephemeral;

    #[test]
    fn write_when_backup_fails() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            // Making the backup page a distant page forces an error.
            match write(tmp.borrow_mut(), (0, 2), &[0u8; page::SIZE-1]) {
                Ok(_) => panic!("allowed backup page failure"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE], buf);
        });
    }

    #[test]
    fn write_when_main_fails() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            // Making the main page a distant page forces an error.
            match write(tmp.borrow_mut(), (2, 0), &[0u8; page::SIZE-1]) {
                Ok(_) => panic!("allowed main page failure"),
                Err(error) => assert_eq!("tried to read distant page", error.to_string()),
            }
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE], buf);
        });
    }

    #[test]
    fn write_without_errors() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            page::write(tmp.borrow_mut(), 1, &[2u8; page::SIZE]).unwrap();

            write(tmp.borrow_mut(), (1, 0), &[3u8; page::SIZE-1]).unwrap();

            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([2u8; page::SIZE], buf);

            page::read(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!(buf[0..page::SIZE-1], [3u8; page::SIZE-1]);
            assert_eq!(integrity::crc(CRC_POLY, &buf[0..page::SIZE-1]), buf[page::SIZE-1]);
        });
    }

    #[test]
    fn read_when_main_is_corrupt() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            write(tmp.borrow_mut(), (0, 1), &[2u8; page::SIZE-1]).unwrap();
            // Overwrite the CRC error detection code at the end of the page.
            page::write(tmp.borrow_mut(), 0, &[4u8; page::SIZE]).unwrap();

            let mut buf = [0u8; page::SIZE-1];
            read(tmp.borrow_mut(), (0, 1), &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE-1], buf);
            // Make sure the backup data is written to the main page.
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE-1], buf[0..page::SIZE-1]);
            assert_eq!(integrity::crc(CRC_POLY, &buf[0..page::SIZE-1]), buf[page::SIZE-1]);
        });

        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            write(tmp.borrow_mut(), (0, 1), &[2u8; page::SIZE-1]).unwrap();
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            // Single byte corruption.
            buf[0] = !buf[0];
            page::write(tmp.borrow_mut(), 0, &buf).unwrap();

            let mut buf = [0u8; page::SIZE-1];
            read(tmp.borrow_mut(), (0, 1), &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE-1], buf);
            // Make sure the backup data is written to the main page.
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE-1], buf[0..page::SIZE-1]);
            assert_eq!(integrity::crc(CRC_POLY, &buf[0..page::SIZE-1]), buf[page::SIZE-1]);
        });
    }

    #[test]
    fn read_when_main_is_intact() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            write(tmp.borrow_mut(), (0, 1), &[2u8; page::SIZE-1]).unwrap();

            let mut buf = [0u8; page::SIZE-1];
            read(tmp.borrow_mut(), (0, 1), &mut buf).unwrap();
            assert_eq!([2u8; page::SIZE-1], buf);
        });
    }

    #[test]
    fn init_when_backup_fails() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            // Making the backup page a distant page forces an error.
            match init(tmp.borrow_mut(), (0, 2)) {
                Ok(_) => panic!("allowed meta init page failure"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([1u8; page::SIZE], buf);
        });
    }

    #[test]
    fn init_when_main_fails() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            // Making the backup page a distant page forces an error.
            match init(tmp.borrow_mut(), (2, 0)) {
                Ok(_) => panic!("allowed meta init failure"),
                Err(error) => assert_eq!("tried to write distant page", error.to_string()),
            }
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!([0u8; page::SIZE-1], buf[0..page::SIZE-1]);
            assert_eq!(integrity::crc(CRC_POLY, &[0u8; page::SIZE-1]), buf[page::SIZE-1]);
        });
    }

    #[test]
    fn init_without_errors() {
        ephemeral::file!(tmp {
            page::write(tmp.borrow_mut(), 0, &[1u8; page::SIZE]).unwrap();
            page::write(tmp.borrow_mut(), 1, &[2u8; page::SIZE]).unwrap();

            init(tmp.borrow_mut(), (1, 0)).unwrap();

            let mut expected = [0u8; page::SIZE];
            expected[page::SIZE-1] = integrity::crc(CRC_POLY, &expected[0..page::SIZE-1]);
            let mut buf = [0u8; page::SIZE];
            page::read(tmp.borrow_mut(), 0, &mut buf).unwrap();
            assert_eq!(expected, buf);
            page::read(tmp.borrow_mut(), 1, &mut buf).unwrap();
            assert_eq!(expected, buf);
        });
    }
}
