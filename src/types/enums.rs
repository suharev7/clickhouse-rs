use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum EnumSize {
    ENUM8,
    ENUM16,
}

#[derive(Clone)]
pub struct Enum(pub(crate) i16);

impl Default for Enum {
    fn default() -> Self {
        Self(0)
    }
}


impl PartialEq for Enum {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl fmt::Display for Enum {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl fmt::Debug for Enum {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Enum({})", self.0)
    }
}

impl Enum {
    pub fn of(source: i16) -> Self {
        Self(source)
    }
    #[inline(always)]
    pub fn internal(&self) -> i16 {
        self.0
    }
}
