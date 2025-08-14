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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tribool() {
        let t = Tribool::True;
        let f = Tribool::False;
        let n = Tribool::Null;

        assert!(t.is_true());
        assert!(!t.is_false());
        assert!(!t.is_null());

        assert!(!f.is_true());
        assert!(f.is_false());
        assert!(!f.is_null());

        assert!(!n.is_true());
        assert!(!n.is_false());
        assert!(n.is_null());
    }

    #[test]
    fn test_conversion_from_bool() {
        assert_eq!(Tribool::from(true), Tribool::True);
        assert_eq!(Tribool::from(false), Tribool::False);
    }

    #[test]
    fn test_not_operator() {
        assert_eq!(!Tribool::True, Tribool::False);
        assert_eq!(!Tribool::False, Tribool::True);
        assert_eq!(!Tribool::Null, Tribool::Null);
    }
}
