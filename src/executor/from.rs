/*
As I understand
central in a join is some kind of evalute the join constraint on two rows,
one from left scan. or earlier joins, and one from right scan.
I seems like a Filter.check(othertable,othercolumns,otherrows)
 will use self.row and use all rows in the chain 'next' to evaluate the join constraint

In my tryouts a managed to use Filter.check(...) properly I think like
Filter(leftrows).check(righttable, rightcolumns, rightrows)

But when later on when I will chain the right row at the end of lefts.next I don't
know how to do.

*/
// snippet here
//a_left_row and a_right_row are of type BlendContext
for a_left_row in &sofar {
  for a_right_row in &next {
    let filter_context = self.filter_context.as_ref().map(Rc::clone);
    let filter_context = a_left_row.concat_into(filter_context);
    let filter = Filter::new(self.storage, join_on_expr, filter_context, None);
    let res = filter.check(
        a_right_row.table_alias,
        &a_right_row.columns,
        &a_right_row.row.as_ref().unwrap(),
    );
    match res {
        Ok(was_joined) => {
            match was_joined {
                true => {
                  // This 'concat_into' is not working
                  a_left_row.concat_into(a_right_row);
                }
                false => {}
            };
        }
        Err(errtxt) => panic!("{}", errtxt),
    };
  }
}
