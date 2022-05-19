use {
    super::Interval,
    std::ops::{Div, Mul},
};

impl Mul<i8> for Interval {
    type Output = Self;

    fn mul(self, rhs: i8) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(v * rhs as i32),
            Interval::Microsecond(v) => Interval::Microsecond(v * rhs as i64),
        }
    }
}

impl Mul<i32> for Interval {
    type Output = Self;

    fn mul(self, rhs: i32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(v * rhs as i64),
        }
    }
}

impl Mul<i64> for Interval {
    type Output = Self;

    fn mul(self, rhs: i64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i64) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v * rhs) as i64),
        }
    }
}

impl Mul<i128> for Interval {
    type Output = Self;

    fn mul(self, rhs: i128) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i128) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as i128) * rhs) as i64),
        }
    }
}

impl Mul<f64> for Interval {
    type Output = Self;

    fn mul(self, rhs: f64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v as f64 * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v as f64 * rhs) as i64),
        }
    }
}

impl Mul<Interval> for i8 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for i32 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for i64 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for i128 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for f64 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Div<i8> for Interval {
    type Output = Self;

    fn div(self, rhs: i8) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(v / rhs as i32),
            Interval::Microsecond(v) => Interval::Microsecond(v / rhs as i64),
        }
    }
}

impl Div<i32> for Interval {
    type Output = Self;

    fn div(self, rhs: i32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(v / rhs),
            Interval::Microsecond(v) => Interval::Microsecond(v / rhs as i64),
        }
    }
}

impl Div<i64> for Interval {
    type Output = Self;

    fn div(self, rhs: i64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i64) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v / rhs) as i64),
        }
    }
}

impl Div<i128> for Interval {
    type Output = Self;

    fn div(self, rhs: i128) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i128) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as i128) / rhs) as i64),
        }
    }
}

impl Div<f64> for Interval {
    type Output = Self;

    fn div(self, rhs: f64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v as f64 / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v as f64 / rhs) as i64),
        }
    }
}

impl Div<Interval> for i8 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month(self as i32 / v),
            Interval::Microsecond(v) => Interval::Microsecond(self as i64 / v),
        }
    }
}

impl Div<Interval> for i32 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month(self / v),
            Interval::Microsecond(v) => Interval::Microsecond(self as i64 / v),
        }
    }
}

impl Div<Interval> for i64 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as i64)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(self / v),
        }
    }
}

impl Div<Interval> for i128 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as i128)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as i128)) as i64),
        }
    }
}

impl Div<Interval> for f64 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / v as f64) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / v as f64) as i64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Interval;

    #[test]
    fn arithmetic() {
        use Interval::*;

        assert_eq!(Month(2) * 3_i8, Month(6));
        assert_eq!(2_i8 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i32, Month(6));
        assert_eq!(2_i32 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i64, Month(6));
        assert_eq!(2_i64 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i128, Month(6));
        assert_eq!(2_i128 * Month(3), Month(6));

        assert_eq!(Month(2) * 3.0, Month(6));
        assert_eq!(2.0 * Month(3), Month(6));

        assert_eq!(Month(6) / 3_i8, Month(2));
        assert_eq!(6_i8 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i32, Month(2));
        assert_eq!(6_i32 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i64, Month(2));
        assert_eq!(6_i64 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i128, Month(2));
        assert_eq!(6_i128 / Month(2), Month(3));

        assert_eq!(Month(8) / 4.0, Month(2));
        assert_eq!(8.0 / Month(4), Month(2));

        assert_eq!(Microsecond(2) * 3_i8, Microsecond(6));
        assert_eq!(2_i8 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i64, Microsecond(6));
        assert_eq!(2_i64 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i128, Microsecond(6));
        assert_eq!(2_i128 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(6) / 3_i8, Microsecond(2));
        assert_eq!(6_i8 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i64, Microsecond(2));
        assert_eq!(6_i64 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i128, Microsecond(2));
        assert_eq!(6_i128 / Microsecond(2), Microsecond(3));
    }
}
