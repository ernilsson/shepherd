use std::{
    error::Error,
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

pub mod slot {
    use std::io;

    use crate::dbms::storage::integrity;

    type Index = u16;

    const CRC_POLY: u8 = 0x07;

    #[derive(Default, Copy, Clone)]
    struct Block {
        offset: Index,
        size: u16,
    }

    impl Block {
        const SIZE: usize = 4;
    }

    fn read_blocks(page: &[u8; super::SIZE]) -> [Block; 5] {
        const OFFSET: usize = 3;
        let mut blocks = [Block::default(); 5];
        for (index, block) in blocks.iter_mut().enumerate() {
            let mut base = OFFSET + Block::SIZE * index;
            block.size = u16::from_le_bytes(page[base..base + 2].try_into().unwrap());
            base += 2;
            block.offset = u16::from_le_bytes(page[base..base + 2].try_into().unwrap());
        }
        blocks
    }

    fn write_blocks(page: &mut [u8; super::SIZE], blocks: &[Block; 5]) {
        const OFFSET: usize = 3;
        for (index, block) in blocks.iter().enumerate() {
            let mut base = OFFSET + Block::SIZE * index;
            page[base..base + 2].copy_from_slice(&block.size.to_le_bytes());
            base += 2;
            page[base..base + 2].copy_from_slice(&block.offset.to_le_bytes());
        }
        write_checksum(page);
    }

    fn write_checksum(page: &mut [u8; super::SIZE]) {
        page[0] = integrity::crc(CRC_POLY, &page[1..]);
    }

    pub fn verify_checksum(page: &[u8; super::SIZE]) -> Result<(), integrity::Error> {
        if page[0] == integrity::crc(CRC_POLY, &page[1..]) {
            Ok(())
        } else {
            Err(integrity::Error::BadChecksum)
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::dbms::storage::page;

        use super::*;

        #[test]
        fn read_blocks_when_partially_filled() {
            let mut page = [0u8; page::SIZE];
            // Medium sized values.
            page[3..5].copy_from_slice(&1265u16.to_le_bytes());
            page[5..7].copy_from_slice(&4032u16.to_le_bytes());
            // Small sized values.
            page[7..9].copy_from_slice(&45u16.to_le_bytes());
            page[9..11].copy_from_slice(&128u16.to_le_bytes());
            // Max sized values.
            page[11..13].copy_from_slice(&u16::MAX.to_le_bytes());
            page[13..15].copy_from_slice(&u16::MAX.to_le_bytes());

            let blocks = read_blocks(&page);
            assert_eq!(blocks[0].size, 1265);
            assert_eq!(blocks[0].offset, 4032);

            assert_eq!(blocks[1].size, 45);
            assert_eq!(blocks[1].offset, 128);

            assert_eq!(blocks[2].size, u16::MAX);
            assert_eq!(blocks[2].offset, u16::MAX);

            // Remaining blocks should be zero'ed out.
            for block in blocks[3..].iter() {
                assert_eq!(block.size, 0);
                assert_eq!(block.offset, 0);
            }

            // Single block.
            let mut page = [0u8; page::SIZE];
            page[3..5].copy_from_slice(&1265u16.to_le_bytes());
            page[5..7].copy_from_slice(&4032u16.to_le_bytes());

            let blocks = read_blocks(&page);
            assert_eq!(blocks[0].size, 1265);
            assert_eq!(blocks[0].offset, 4032);

            // Remaining blocks should be zero'ed out.
            for block in blocks[1..].iter() {
                assert_eq!(block.size, 0);
                assert_eq!(block.offset, 0);
            }
        }

        #[test]
        fn read_blocks_when_filled() {
            let mut page = [0u8; page::SIZE];

            page[3..5].copy_from_slice(&1265u16.to_le_bytes());
            page[5..7].copy_from_slice(&4032u16.to_le_bytes());

            page[7..9].copy_from_slice(&45u16.to_le_bytes());
            page[9..11].copy_from_slice(&128u16.to_le_bytes());

            page[11..13].copy_from_slice(&u16::MAX.to_le_bytes());
            page[13..15].copy_from_slice(&u16::MAX.to_le_bytes());

            page[15..17].copy_from_slice(&34444u16.to_le_bytes());
            page[17..19].copy_from_slice(&12334u16.to_le_bytes());

            page[19..21].copy_from_slice(&21123u16.to_le_bytes());
            page[21..23].copy_from_slice(&0u16.to_le_bytes());

            let blocks = read_blocks(&page);
            assert_eq!(blocks[0].size, 1265);
            assert_eq!(blocks[0].offset, 4032);

            assert_eq!(blocks[1].size, 45);
            assert_eq!(blocks[1].offset, 128);

            assert_eq!(blocks[2].size, u16::MAX);
            assert_eq!(blocks[2].offset, u16::MAX);

            assert_eq!(blocks[3].size, 34444);
            assert_eq!(blocks[3].offset, 12334);

            assert_eq!(blocks[4].size, 21123);
            assert_eq!(blocks[4].offset, 0);
        }

        #[test]
        fn read_blocks_when_empty() {
            let mut page = [0u8; page::SIZE];
            for block in read_blocks(&page) {
                assert_eq!(block.size, 0);
                assert_eq!(block.offset, 0);
            }
        }

        #[test]
        fn write_blocks_when_partially_filled() {
            let mut blocks = [Block::default(); 5];
            blocks[0].offset = 1234;
            blocks[0].size = 1034;
            let mut page = [0u8; page::SIZE];
            write_blocks(&mut page, &blocks);
            assert_eq!(page[0], integrity::crc(CRC_POLY, &page[1..]));
            assert_eq!(page[3..5], 1034u16.to_le_bytes());
            assert_eq!(page[5..7], 1234u16.to_le_bytes());
            assert_eq!(page[7..], [0u8; page::SIZE - 7]);

            blocks[2].offset = u16::MAX;
            blocks[2].size = u16::MAX;
            write_blocks(&mut page, &blocks);
            assert_eq!(page[0], integrity::crc(CRC_POLY, &page[1..]));
            assert_eq!(page[3..5], 1034u16.to_le_bytes());
            assert_eq!(page[5..7], 1234u16.to_le_bytes());
            assert_eq!(page[7..9], 0u16.to_le_bytes());
            assert_eq!(page[9..11], 0u16.to_le_bytes());
            assert_eq!(page[11..13], u16::MAX.to_le_bytes());
            assert_eq!(page[13..15], u16::MAX.to_le_bytes());
        }

        #[test]
        fn write_blocks_when_filled() {
            let mut blocks = [Block::default(); 5];
            for (index, block) in blocks.iter_mut().enumerate() {
                block.size = (index as u16 + 1) * 100;
                block.offset = (index as u16 + 1) * 100;
            }
            let mut page = [0u8; page::SIZE];
            write_blocks(&mut page, &blocks);
            assert_eq!(page[0], integrity::crc(CRC_POLY, &page[1..]));
            for (index, block) in blocks.iter_mut().enumerate() {
                let mut offset = 3 + index * 4;
                assert_eq!(page[offset..offset + 2], block.size.to_le_bytes());
                offset += 2;
                assert_eq!(page[offset..offset + 2], block.offset.to_le_bytes());
            }
            assert_eq!(page[3 + 4 * 5..], [0u8; page::SIZE - (3 + 4 * 5)]);
        }

        #[test]
        fn write_blocks_when_empty() {
            let mut blocks = [Block::default(); 5];
            let mut page = [0u8; page::SIZE];
            write_blocks(&mut page, &blocks);
            // At this point all bytes should be zero'd out, which gives a
            // checksum of zero as well.
            assert_eq!([0u8; page::SIZE], page);
        }
    }
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
