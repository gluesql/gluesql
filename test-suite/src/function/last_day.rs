use {
    crate::*,
    chrono::NaiveDate,
    gluesql_core::prelude::Value::*,
};


test_case!(last_day, async move {
    /// 추가해야 하는 테스트를 생각해보자
    /// SELECT LAST_DAY("2017-06-20");
    /// SELECT LAST_DAY("2017-02-10 09:34:00");
    /// - Date 혹은 DateTime을 주었을 때 해당 Date가 포함된 월의 마지막 날짜를 잘 반환하는지
    /// - 반환된 날짜로 테이블에 insert가 되는지
    /// - Date 타입이 맞긴 하지만, 존재할 수 없는 날짜를 주었을 때 에러가 잘 발생하는지
    /// - Date 혹은 DateTime이 아닌 다른 타입을 주었을 때 에러가 잘 발생하는지
    ///
    /// 궁금한 지점
    /// - 날짜를 어떤 형식까지 받아들여야 하는 걸까?
    /// - 그냥 yyyy-mm-dd만 받게 하는 건 어떨까?
    
    
    test! {
        name: "Should properly return the last day of the month of a given date",
        sql: "VALUES(LAST_DAY('2017-06-15'));",
        expected: Ok(select!(
            column1
            Date;
            NaiveDate::from_ymd_opt(2017, 6, 30).unwrap()
        ))
    };

    test! {
        name: "Should properly return the last day of the month of a given date even if the given month is December",
        sql: "VALUES(LAST_DAY('2017-12-15'));",
        expected: Ok(select!(
            column1
            Date;
            NaiveDate::from_ymd_opt(2017, 12, 31).unwrap()
        ))
    };
});
