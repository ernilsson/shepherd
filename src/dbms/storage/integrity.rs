use std::ops::BitOr;

#[derive(Debug, PartialEq)]
enum Bit {
    Set,
    Unset,
}

impl From<u8> for Bit {
    fn from(n: u8) -> Self {
        match n {
            1 => Bit::Set,
            0 => Bit::Unset,
            _ => panic!("cannot interpret {} as binary value", n),
        }
    }
}

impl BitOr<u8> for Bit {
    type Output = u8;

    fn bitor(self, rhs: u8) -> Self::Output {
        match self {
            Bit::Set => 1 | rhs,
            Bit::Unset => rhs,
        }
    }
}

struct Register<'a> {
    current: u8,
    n: &'a [u8],
    cursor: (u8, usize),
}

impl<'a> Register<'a> {
    fn new(n: &'a [u8]) -> Self {
        Self {
            current: if n.is_empty() { 0 } else { n[0] },
            n,
            cursor: (7, 1),
        }
    }

    fn shift(&mut self) -> Option<Bit> {
        if self.cursor.1 > self.n.len() {
            return None;
        }
        let bit = Bit::from(self.current >> 7);
        self.current = self.pop() | (self.current << 1);
        Some(bit)
    }

    fn pop(&mut self) -> Bit {
        let bit = if self.cursor.1 < self.n.len() {
            Bit::from(self.n[self.cursor.1] >> self.cursor.0 & 1)
        } else {
            Bit::Unset
        };
        self.advance();
        bit
    }

    fn advance(&mut self) {
        if self.cursor.0 == 0 {
            self.cursor.1 += 1;
            self.cursor.0 = 7;
        } else {
            self.cursor.0 -= 1;
        }
    }

    fn xor(&mut self, n: u8) {
        self.current ^= n
    }

    fn get(self) -> u8 {
        self.current
    }
}

pub fn crc(poly: u8, n: &[u8]) -> u8 {
    let mut register = Register::new(n);
    while let Some(bit) = register.shift() {
        if Bit::Set == bit {
            register.xor(poly);
        }
    }
    register.get()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_new_given_empty_n() {
        let n: [u8; 0] = [];
        let register = Register::new(&n);
        assert_eq!(0, register.current);
    }

    #[test]
    fn register_new_given_non_empty_n() {
        let n = [1u8];
        let register = Register::new(&n);
        assert_eq!(1, register.current);

        let n = [3u8, 2, 1];
        let register = Register::new(&n);
        assert_eq!(3, register.current);
    }

    #[test]
    fn register_shift() {
        let n = [0b0000_0000u8];
        let mut register = Register::new(&n);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0, register.current);

        let n = [0b1000_0000u8];
        let mut register = Register::new(&n);
        assert_eq!(Bit::Set, register.shift().unwrap());
        assert_eq!(0, register.current);

        let n = [0b1100_0000u8];
        let mut register = Register::new(&n);
        assert_eq!(Bit::Set, register.shift().unwrap());
        assert_eq!(0b1000_0000u8, register.current);

        let n = [0b1100_0000u8, 0b1000_0000u8];
        let mut register = Register::new(&n);
        assert_eq!(Bit::Set, register.shift().unwrap());
        assert_eq!(0b1000_0001u8, register.current);
        assert_eq!(Bit::Set, register.shift().unwrap());
        assert_eq!(0b0000_0010u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0100u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_1000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0001_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0010_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0100_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b1000_0000u8, register.current);
        assert_eq!(Bit::Set, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert_eq!(Bit::Unset, register.shift().unwrap());
        assert_eq!(0b0000_0000u8, register.current);
        assert!(register.shift().is_none());
    }

    #[test]
    fn crc_with_empty_n() {
        let n: [u8; 0] = [];
        assert_eq!(0, crc(0xFFu8, &n));
    }

    #[test]
    fn crc_with_smbus_poly() {
        let n = [0xAB, 0xCD, 0xEF];
        assert_eq!(0x23, crc(0x07, &n));
    }

    #[test]
    fn crc_with_opensafety_poly() {
        let n = [0xAB, 0xCD, 0xEF, 0xAA, 0xBB, 0xCC];
        assert_eq!(0xB0, crc(0x2F, &n));
    }
}
