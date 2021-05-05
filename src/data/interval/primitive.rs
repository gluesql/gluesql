use {
    super::Interval,
    std::ops::{Div, Mul},
};

impl Mul<i64> for Interval {
    type Output = Self;

    fn mul(self, rhs: i64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i64) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v * rhs) as i64),
        }
    }
}

impl Mul<Interval> for i64 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
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

impl Div<i64> for Interval {
    type Output = Self;

    fn div(self, rhs: i64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i64) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v / rhs) as i64),
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

impl Div<f64> for Interval {
    type Output = Self;

    fn div(self, rhs: f64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v as f64 / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v as f64 / rhs) as i64),
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

impl Mul<Interval> for f64 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

#[cfg(test)]
mod tests {
    use super::Interval;

    #[test]
    fn arithmetic() {
        use Interval::*;

        assert_eq!(Month(2) * 3, Month(6));
        assert_eq!(Month(6), Month(3) * 2);
        assert_eq!(Month(2) * 3.0, Month(6));
        assert_eq!(Month(6), Month(3) * 2.0);
        assert_eq!(Month(6) / 3, Month(2));
        assert_eq!(Month(3), Month(6) / 2);
        assert_eq!(Month(8) / 4.0, Month(2));
        assert_eq!(Month(4), Month(8) / 2.0);
    }
}
