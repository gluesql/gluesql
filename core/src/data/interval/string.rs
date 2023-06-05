use {
    super::{Interval, IntervalError, DAY, HOUR, MINUTE, SECOND},
    crate::{
        ast::{Expr, ToSql},
        parse_sql::parse_interval,
        result::Result,
        translate::translate_expr,
    },
};

impl Interval {
    pub fn parse(s: &str) -> Result<Self> {
        let parsed = parse_interval(s)?;

        match translate_expr(&parsed)? {
            Expr::Interval {
                expr,
                leading_field,
                last_field,
            } => {
                let literal = match expr.as_ref() {
                    Expr::Literal(literal) => literal.to_sql(),
                    _ => {
                        return Err(IntervalError::ParseSupportedOnlyLiteral { expr: *expr }.into())
                    }
                };

                Interval::try_from_str(&literal, leading_field, last_field)
            }
            _ => Err(IntervalError::Unreachable.into()),
        }
    }

    pub fn to_sql_str(&self) -> String {
        match self {
            Interval::Month(v) => {
                let v = *v;
                let (sign, v) = if v < 0 { ("-", -v) } else { ("", v) };

                let year = v / 12;
                let month = v % 12;

                match (year, month) {
                    (_, 0) if year != 0 => format!("'{}{}' YEAR", sign, year),
                    (0, _) => format!("'{}{}' MONTH", sign, month),
                    _ => format!("'{}{}-{}' YEAR TO MONTH", sign, year, month),
                }
            }
            Interval::Microsecond(v) => {
                let v = *v;
                let (sign, v) = if v < 0 { ("-", -v) } else { ("", v) };

                let day = v / DAY;
                let hour = (v % DAY) / HOUR;
                let minute = (v % HOUR) / MINUTE;
                let second = (v % MINUTE) / SECOND;
                let microsecond = v % SECOND;

                macro_rules! micro {
                    () => {
                        format!("{:06}", microsecond).trim_end_matches('0')
                    };
                }

                macro_rules! f {
                    ($template: literal; $( $value: expr )*; $from_to: literal) => {
                        format!("'{}{}' {}", sign, format!($template, $( $value ),*), $from_to)
                    };

                    (DAY $template: literal) => {
                        f!($template; day; "DAY")
                    };
                    (HOUR $template: literal) => {
                        f!($template; hour; "HOUR")
                    };
                    (MINUTE $template: literal) => {
                        f!($template; minute; "MINUTE")
                    };
                    (SECOND $template: literal) => {
                        f!($template; second; "SECOND")
                    };
                    (MICRO $template: literal) => {
                        f!($template; second micro!(); "SECOND")
                    };

                    // MINUTE TO ..
                    (MINUTE TO SECOND $template: literal) => {
                        f!($template; minute second; "MINUTE TO SECOND")
                    };
                    (MINUTE TO MICRO $template: literal) => {
                        f!($template; minute second micro!(); "MINUTE TO SECOND")
                    };

                    // HOUR TO ..
                    (HOUR TO MINUTE $template: literal) => {
                        f!($template; hour minute; "HOUR TO MINUTE")
                    };
                    (HOUR TO SECOND $template: literal) => {
                        f!($template; hour minute second; "HOUR TO SECOND")
                    };
                    (HOUR TO MICRO $template: literal) => {
                        f!($template; hour minute second micro!(); "HOUR TO SECOND")
                    };

                    // DAY TO ..
                    (DAY TO HOUR $template: literal) => {
                        f!($template; day hour; "DAY TO HOUR")
                    };
                    (DAY TO MINUTE $template: literal) => {
                        f!($template; day hour minute; "DAY TO MINUTE")
                    };
                    (DAY TO SECOND $template: literal) => {
                        f!($template; day hour minute second; "DAY TO SECOND")
                    };
                    (DAY TO MICRO $template: literal) => {
                        f!($template; day hour minute second micro!(); "DAY TO SECOND")
                    };
                }

                match (day, hour, minute, second, microsecond) {
                    (_, 0, 0, 0, 0) if day != 0 => f!(DAY "{}"),
                    (0, _, 0, 0, 0) if hour != 0 => f!(HOUR "{}"),
                    (0, 0, _, 0, 0) if minute != 0 => f!(MINUTE "{}"),
                    (0, 0, 0, _, 0) if second != 0 => f!(SECOND "{}"),
                    (0, 0, 0, _, _) if microsecond != 0 => f!(MICRO "{}.{}"),

                    // MINUTE TO ..
                    (0, 0, _, _, 0) => f!(MINUTE TO SECOND "{:02}:{:02}"),
                    (0, 0, _, _, _) => f!(MINUTE TO MICRO  "{:02}:{:02}.{}"),

                    // HOUR TO ..
                    (0, _, _, 0, 0) => f!(HOUR TO MINUTE "{:02}:{:02}"),
                    (0, _, _, _, 0) => f!(HOUR TO SECOND "{:02}:{:02}:{:02}"),
                    (0, _, _, _, _) => f!(HOUR TO MICRO  "{:02}:{:02}:{:02}.{}"),

                    // DAY TO ..
                    (_, _, 0, 0, 0) => f!(DAY TO HOUR   "{} {}"),
                    (_, _, _, 0, 0) => f!(DAY TO MINUTE "{} {:02}:{:02}"),
                    (_, _, _, _, 0) => f!(DAY TO SECOND "{} {:02}:{:02}:{:02}"),
                    (_, _, _, _, _) => f!(DAY TO MICRO  "{} {:02}:{:02}:{:02}.{}"),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Interval;

    #[test]
    fn parse() {
        macro_rules! test {
            ($( $value: literal $duration: ident ),* => $result: literal $from_to: tt) => {
                let interval = interval!($( $value $duration ),*);
                let interval_str = format!("'{}' {}", $result, stringify!($from_to));

                let expected = Interval::parse(interval_str.as_str());
                assert_eq!(Ok(interval), expected);
                assert_eq!(interval.to_sql_str(), interval_str);
            };
            ($( $value: literal $duration: ident ),* => $result: literal $from: tt TO $to: tt) => {
                let interval = interval!($( $value $duration ),*);
                let interval_str = format!(
                    "'{}' {} TO {}",
                    $result,
                    stringify!($from),
                    stringify!($to),
                );

                let expected = Interval::parse(interval_str.as_str());
                assert_eq!(Ok(interval), expected);
                assert_eq!(interval.to_sql_str(), interval_str);
            };
        }

        macro_rules! interval {
            ($value: literal $duration:ident) => {
                Interval::$duration($value)
            };
            ($interval: expr ; $value: literal $duration:ident) => {
                $interval.add(&Interval::$duration($value)).unwrap()
            };
            ($v0: literal $d0: ident, $( $v1: literal $d1: ident ),*) => {
                interval!(
                    Interval::$d0($v0) ;
                    $( $v1 $d1 ),*
                )
            };
            ($interval: expr ; $v0: literal $d0: ident, $( $v1: literal $d1: ident ),*) => {
                interval!(
                    $interval.add(&Interval::$d0($v0)).unwrap() ;
                    $( $v1 $d1 ),*
                )
            };
        }

        // YEAR and MONTH
        test!(33 years            => "33" YEAR);
        test!(-33 years           => "-33" YEAR);
        test!(14 months           => "1-2" YEAR TO MONTH);
        test!(-33 months          => "-2-9" YEAR TO MONTH);
        test!(12 months           => "1" YEAR);
        test!(11 months           => "11" MONTH);
        test!(-3 months           => "-3" MONTH);
        test!(33 years, 11 months => "33-11" YEAR TO MONTH);
        test!(1 years, 100 months => "9-4" YEAR TO MONTH);

        // DAY, HOUR, MINUTE and SECOND
        test!(102 days          => "102" DAY);
        test!(-20 days          => "-20" DAY);
        test!(30 hours          => "1 6" DAY TO HOUR);
        test!(24 hours          => "1" DAY);
        test!(3 hours           => "3" HOUR);
        test!(-5 hours          => "-5" HOUR);
        test!(350 minutes       => "05:50" HOUR TO MINUTE);
        test!(600 minutes       => "10" HOUR);
        test!(59 minutes        => "59" MINUTE);
        test!(3 minutes         => "3" MINUTE);
        test!(-9 minutes        => "-9" MINUTE);
        test!(7298 seconds      => "02:01:38" HOUR TO SECOND);
        test!(98 seconds        => "01:38" MINUTE TO SECOND);
        test!(120 seconds       => "2" MINUTE);
        test!(30 seconds        => "30" SECOND);
        test!(3 seconds         => "3" SECOND);
        test!(-37 seconds       => "-37" SECOND);
        test!(1234 milliseconds => "1.234" SECOND);
        test!(-234 milliseconds  => "-0.234" SECOND);
        test!(1234 microseconds => "0.001234" SECOND);

        // MINUTE TO ..
        test!(11 minutes, 30 seconds                     => "11:30" MINUTE TO SECOND);
        test!(-11 minutes, -39 seconds                   => "-11:39" MINUTE TO SECOND);
        test!(11 minutes, 36540 microseconds             => "11:00.03654" MINUTE TO SECOND);
        test!(-30 minutes, -9876 microseconds            => "-30:00.009876" MINUTE TO SECOND);
        test!(11 minutes, 30 seconds, 36540 microseconds => "11:30.03654" MINUTE TO SECOND);

        // HOUR TO ..
        test!(11 hours, 50 minutes                   => "11:50" HOUR TO MINUTE);
        test!(-20 hours, -39 minutes                 => "-20:39" HOUR TO MINUTE);
        test!(23 hours, 59 seconds                   => "23:00:59" HOUR TO SECOND);
        test!(-23 hours, -59 seconds                 => "-23:00:59" HOUR TO SECOND);
        test!(7 hours, 30040 microseconds            => "07:00:00.03004" HOUR TO SECOND);
        test!(-7 hours, -30040 microseconds          => "-07:00:00.03004" HOUR TO SECOND);
        test!(23 hours, 12 minutes, 59 seconds       => "23:12:59" HOUR TO SECOND);
        test!(23 hours, 12 minutes, 300 microseconds => "23:12:00.0003" HOUR TO SECOND);
        test!(
            23 hours, 12 minutes, 9 seconds, 300 microseconds =>
            "23:12:09.0003" HOUR TO SECOND
        );

        // DAY TO ..
        test!(1 days, 1 hours                    => "1 1" DAY TO HOUR);
        test!(-1 days, -7 hours                  => "-1 7" DAY TO HOUR);
        test!(30 days, 5 minutes                 => "30 00:05" DAY TO MINUTE);
        test!(-1 days, -130 minutes              => "-1 02:10" DAY TO MINUTE);
        test!(30 days, 100 seconds               => "30 00:01:40" DAY TO SECOND);
        test!(-5 days, -3800 seconds             => "-5 01:03:20" DAY TO SECOND);
        test!(1 days, 1 microseconds             => "1 00:00:00.000001" DAY TO SECOND);
        test!(-1 days, -1 microseconds           => "-1 00:00:00.000001" DAY TO SECOND);
        test!(100 days, 2 hours, 3 minutes       => "100 02:03" DAY TO MINUTE);
        test!(2 days, 20 hours, 3 seconds        => "2 20:00:03" DAY TO SECOND);
        test!(2 days, 20 hours, 234 milliseconds => "2 20:00:00.234" DAY TO SECOND);
        test!(
            30 days, 30 hours, 100 seconds, 3 microseconds =>
            "31 06:01:40.000003" DAY TO SECOND
        );
    }
}
