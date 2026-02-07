#[cfg(test)]
#[macro_export]
macro_rules! file {
    ($name: ident $body: block) => {{
        let mut $name = $crate::dbms::storage::ethemeral::File::new(
            format!(
                "{}-{}.test",
                module_path!().replace("::", "-"),
                rand::random::<u32>()
            )
            .to_string(),
        )
        .unwrap();
        $body
    }};
}

#[allow(unused_imports)]
pub(crate) use file;

#[cfg(test)]
#[derive(Debug)]
pub struct File {
    handle: std::fs::File,
    path: String,
}

#[cfg(test)]
impl File {
    pub fn new(path: String) -> std::io::Result<Self> {
        Ok(Self {
            handle: std::fs::File::options()
                .read(true)
                .write(true)
                .create_new(true)
                .truncate(true)
                .open(&path)?,
            path,
        })
    }

    pub fn borrow_mut(&mut self) -> &mut std::fs::File {
        &mut self.handle
    }
}

#[cfg(test)]
impl Drop for File {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        io::{Read, Seek, Write},
    };

    #[test]
    #[allow(unused_mut)]
    fn test_macro() {
        file!(tmp {
            assert!(tmp.path.starts_with("shepherd-dbms-storage-ethemeral"));
        });
    }

    #[test]
    #[allow(unused_mut)]
    fn file_is_deleted_when_dropped() {
        let path;
        file!(tmp {
            path = tmp.path.clone();
        });
        match fs::exists(&path) {
            Ok(true) => panic!("volatile file {path} exists after being dropped"),
            Err(error) => panic!("error returned when checking volatile file existence {error}"),
            _ => {}
        }
    }

    #[test]
    fn file_is_writable() {
        file!(tmp {
            assert!(tmp.borrow_mut().write_all(&[0u8; 16]).is_ok());
        });
    }

    #[test]
    fn file_is_readable() {
        file!(tmp {
            tmp.borrow_mut().write_all(&[1u8; 16]).unwrap();
            tmp.borrow_mut().seek(std::io::SeekFrom::Start(0)).unwrap();

            let mut read_buffer = [0u8; 16];
            assert!(tmp.borrow_mut().read(&mut read_buffer).is_ok());
            assert_eq!(read_buffer, [1u8; 16]);
        });
    }
}
