//! Submodule of the test suite for the use of SQL triggers in GlueSQL.
use crate::*;

test_case!(trigger, {
    let g = get_tester!();

    // First, we create a table on which afterwards we will associate a trigger.
    g.run(
        r#"CREATE TABLE emp (
        empname           text,
        salary            integer,
        last_date         timestamp,
        last_user         text
    );
    "#,
    )
    .await;

    // We create a function that we will use in the trigger.
    g.run(
        r#"
    CREATE FUNCTION emp_stamp() RETURNS trigger AS $emp_stamp$
        BEGIN
            -- Check that empname and salary are given
            IF NEW.empname IS NULL THEN
                RAISE EXCEPTION 'empname cannot be null';
            END IF;
            IF NEW.salary IS NULL THEN
                RAISE EXCEPTION '% cannot have null salary', NEW.empname;
            END IF;
    
            -- Who works for us when they must pay for it?
            IF NEW.salary < 0 THEN
                RAISE EXCEPTION '% cannot have a negative salary', NEW.empname;
            END IF;
    
            -- Remember who changed the payroll when
            NEW.last_date := current_timestamp;
            NEW.last_user := current_user;
            RETURN NEW;
        END;
    $emp_stamp$ LANGUAGE plpgsql;
    "#,
    )
    .await;

    // We create a trigger that will be run when a row is inserted into the table.
    g.run(
        r#"
        CREATE TRIGGER TriggerTableTrigger
        AFTER INSERT ON emp
        FOR EACH ROW EXECUTE FUNCTION emp_stamp();
        "#,
    )
    .await;
});
