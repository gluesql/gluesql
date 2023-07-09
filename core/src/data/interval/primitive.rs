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

impl Mul<i16> for Interval {
    type Output = Self;

    fn mul(self, rhs: i16) -> Self {
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
            Interval::Month(v) => Interval::Month(v * rhs),
            Interval::Microsecond(v) => Interval::Microsecond(v * rhs as i64),
        }
    }
}

impl Mul<i64> for Interval {
    type Output = Self;

    fn mul(self, rhs: i64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as i64) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(v * rhs),
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

impl Mul<u8> for Interval {
    type Output = Self;

    fn mul(self, rhs: u8) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u8) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u8) * rhs) as i64),
        }
    }
}

impl Mul<u16> for Interval {
    type Output = Self;

    fn mul(self, rhs: u16) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u16) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u16) * rhs) as i64),
        }
    }
}

impl Mul<u32> for Interval {
    type Output = Self;

    fn mul(self, rhs: u32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u32) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u32) * rhs) as i64),
        }
    }
}

impl Mul<u64> for Interval {
    type Output = Self;

    fn mul(self, rhs: u64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u64) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u64) * rhs) as i64),
        }
    }
}

impl Mul<u128> for Interval {
    type Output = Self;

    fn mul(self, rhs: u128) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u128) * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u128) * rhs) as i64),
        }
    }
}

impl Mul<f32> for Interval {
    type Output = Self;

    fn mul(self, rhs: f32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v as f32 * rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v as f32 * rhs) as i64),
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

impl Mul<Interval> for i16 {
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

impl Mul<Interval> for u8 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for u16 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for u32 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for u64 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for u128 {
    type Output = Interval;

    fn mul(self, rhs: Interval) -> Interval {
        rhs * self
    }
}

impl Mul<Interval> for f32 {
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

impl Div<i16> for Interval {
    type Output = Self;

    fn div(self, rhs: i16) -> Self {
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
            Interval::Microsecond(v) => Interval::Microsecond(v / rhs),
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

impl Div<u8> for Interval {
    type Output = Self;

    fn div(self, rhs: u8) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u8) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u8) / rhs) as i64),
        }
    }
}

impl Div<u16> for Interval {
    type Output = Self;

    fn div(self, rhs: u16) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u16) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u16) / rhs) as i64),
        }
    }
}

impl Div<u32> for Interval {
    type Output = Self;

    fn div(self, rhs: u32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u32) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u32) / rhs) as i64),
        }
    }
}

impl Div<u64> for Interval {
    type Output = Self;

    fn div(self, rhs: u64) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u64) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u64) / rhs) as i64),
        }
    }
}

impl Div<u128> for Interval {
    type Output = Self;

    fn div(self, rhs: u128) -> Self {
        match self {
            Interval::Month(v) => Interval::Month(((v as u128) / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond(((v as u128) / rhs) as i64),
        }
    }
}

impl Div<f32> for Interval {
    type Output = Self;

    fn div(self, rhs: f32) -> Self {
        match self {
            Interval::Month(v) => Interval::Month((v as f32 / rhs) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((v as f32 / rhs) as i64),
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

impl Div<Interval> for i16 {
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

impl Div<Interval> for u8 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as u8)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as u8)) as i64),
        }
    }
}

impl Div<Interval> for u16 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as u16)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as u16)) as i64),
        }
    }
}

impl Div<Interval> for u32 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as u32)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as u32)) as i64),
        }
    }
}

impl Div<Interval> for u64 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as u64)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as u64)) as i64),
        }
    }
}

impl Div<Interval> for u128 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / (v as u128)) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / (v as u128)) as i64),
        }
    }
}

impl Div<Interval> for f32 {
    type Output = Interval;

    fn div(self, rhs: Interval) -> Interval {
        match rhs {
            Interval::Month(v) => Interval::Month((self / v as f32) as i32),
            Interval::Microsecond(v) => Interval::Microsecond((self / v as f32) as i64),
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

        assert_eq!(Month(2) * 3_i16, Month(6));
        assert_eq!(2_i16 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i32, Month(6));
        assert_eq!(2_i32 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i64, Month(6));
        assert_eq!(2_i64 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_i128, Month(6));
        assert_eq!(2_i128 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_u8, Month(6));
        assert_eq!(2_u8 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_u16, Month(6));
        assert_eq!(2_u16 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_u32, Month(6));
        assert_eq!(2_u32 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_u64, Month(6));
        assert_eq!(2_u64 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_u128, Month(6));
        assert_eq!(2_u128 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_f32, Month(6));
        assert_eq!(2_f32 * Month(3), Month(6));

        assert_eq!(Month(2) * 3_f64, Month(6));
        assert_eq!(2_f64 * Month(3), Month(6));

        assert_eq!(Month(2) * 3.0, Month(6));
        assert_eq!(2.0 * Month(3), Month(6));

        assert_eq!(Month(6) / 3_i8, Month(2));
        assert_eq!(6_i8 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i16, Month(2));
        assert_eq!(6_i16 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i32, Month(2));
        assert_eq!(6_i32 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i64, Month(2));
        assert_eq!(6_i64 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_i128, Month(2));
        assert_eq!(6_i128 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_u8, Month(2));
        assert_eq!(6_u8 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_u16, Month(2));
        assert_eq!(6_u16 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_u32, Month(2));
        assert_eq!(6_u32 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_u64, Month(2));
        assert_eq!(6_u64 / Month(2), Month(3));

        assert_eq!(Month(6) / 3_u128, Month(2));
        assert_eq!(6_u128 / Month(2), Month(3));

        assert_eq!(Month(8) / 4.0_f32, Month(2));
        assert_eq!(8.0_f32 / Month(4), Month(2));

        assert_eq!(Month(8) / 4.0, Month(2));
        assert_eq!(8.0 / Month(4), Month(2));

        assert_eq!(Microsecond(2) * 3_i8, Microsecond(6));
        assert_eq!(2_i8 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i16, Microsecond(6));
        assert_eq!(2_i16 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i32, Microsecond(6));
        assert_eq!(2_i32 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i64, Microsecond(6));
        assert_eq!(2_i64 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_i128, Microsecond(6));
        assert_eq!(2_i128 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_u8, Microsecond(6));
        assert_eq!(2_u8 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_u16, Microsecond(6));
        assert_eq!(2_u16 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_u32, Microsecond(6));
        assert_eq!(2_u32 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_u64, Microsecond(6));
        assert_eq!(2_u64 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3_u128, Microsecond(6));
        assert_eq!(2_u128 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3.0_f32, Microsecond(6));
        assert_eq!(2.0_f32 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(2) * 3.0, Microsecond(6));
        assert_eq!(2.0 * Microsecond(3), Microsecond(6));

        assert_eq!(Microsecond(6) / 3_i8, Microsecond(2));
        assert_eq!(6_i8 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i16, Microsecond(2));
        assert_eq!(6_i16 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i32, Microsecond(2));
        assert_eq!(6_i32 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i64, Microsecond(2));
        assert_eq!(6_i64 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_i128, Microsecond(2));
        assert_eq!(6_i128 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_u8, Microsecond(2));
        assert_eq!(6_u8 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_u16, Microsecond(2));
        assert_eq!(6_u16 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_u32, Microsecond(2));
        assert_eq!(6_u32 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_u64, Microsecond(2));
        assert_eq!(6_u64 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_u128, Microsecond(2));
        assert_eq!(6_u128 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_f32, Microsecond(2));
        assert_eq!(6_f32 / Microsecond(2), Microsecond(3));

        assert_eq!(Microsecond(6) / 3_f64, Microsecond(2));
        assert_eq!(6_f64 / Microsecond(2), Microsecond(3));
    }
}
