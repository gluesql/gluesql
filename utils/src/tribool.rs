#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tribool {
    True,
    False,
    Null,
}

impl Tribool {
    pub fn is_true(self) -> bool {
        matches!(self, Self::True)
    }

    pub fn is_false(self) -> bool {
        matches!(self, Self::False)
    }

    pub fn is_null(self) -> bool {
        matches!(self, Self::Null)
    }
}

impl std::ops::Not for Tribool {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::True => Self::False,
            Self::False => Self::True,
            Self::Null => Self::Null,
        }
    }
}

impl From<bool> for Tribool {
    fn from(v: bool) -> Self {
        if v { Self::True } else { Self::False }
    }
}
