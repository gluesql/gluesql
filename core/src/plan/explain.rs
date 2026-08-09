use {super::QueryPlan, std::fmt::Display};

pub fn explain(query: &QueryPlan) -> Vec<String> {
    explain_lines(query)
}

pub(crate) trait Explain {
    type Output;

    fn explain(&self, context: &mut ExplainContext) -> Self::Output;
}

#[derive(Default)]
pub(crate) struct ExplainContext {
    next_subquery_id: usize,
    subqueries: Vec<ExplainNode>,
}

#[derive(Clone, Copy)]
pub(crate) enum ExplainSubqueryMode {
    OneRow,
    AllRows,
    Exists,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ExplainNode {
    name: String,
    annotation: Option<String>,
    properties: Vec<ExplainProperty>,
    children: Vec<ExplainNode>,
}

#[derive(Debug, PartialEq, Eq)]
struct ExplainProperty {
    key: &'static str,
    value: String,
}

impl ExplainNode {
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            annotation: None,
            properties: Vec::new(),
            children: Vec::new(),
        }
    }

    pub(crate) fn with_annotation(mut self, annotation: impl Display) -> Self {
        self.annotation = Some(annotation.to_string());
        self
    }

    pub(crate) fn with_property(mut self, key: &'static str, value: impl Display) -> Self {
        self.properties.push(ExplainProperty {
            key,
            value: value.to_string(),
        });
        self
    }

    pub(crate) fn with_optional_property<T: Display>(
        self,
        key: &'static str,
        value: Option<T>,
    ) -> Self {
        match value {
            Some(value) => self.with_property(key, value),
            None => self,
        }
    }

    pub(crate) fn with_child(mut self, child: ExplainNode) -> Self {
        self.children.push(child);
        self
    }

    pub(crate) fn with_children(mut self, children: impl IntoIterator<Item = ExplainNode>) -> Self {
        self.children.extend(children);
        self
    }
}

impl ExplainContext {
    pub(crate) fn subquery_count(&self) -> usize {
        self.subqueries.len()
    }

    pub(crate) fn register_subquery(
        &mut self,
        query: &QueryPlan,
        mode: ExplainSubqueryMode,
    ) -> String {
        self.next_subquery_id += 1;
        let id = format!("@S{}", self.next_subquery_id);
        let insertion_index = self.subqueries.len();

        let plan = query.explain(self);
        self.subqueries.insert(
            insertion_index,
            ExplainNode::new("subquery")
                .with_property("id", &id)
                .with_property("exec mode", mode)
                .with_child(plan),
        );

        id
    }

    fn with_subqueries(self, main: ExplainNode) -> ExplainNode {
        if self.subqueries.is_empty() {
            return main;
        }

        ExplainNode::new("root").with_children(std::iter::once(main).chain(self.subqueries))
    }
}

impl Display for ExplainSubqueryMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::OneRow => "one row",
            Self::AllRows => "all rows",
            Self::Exists => "exists",
        })
    }
}

fn explain_lines(explainable: &impl Explain<Output = ExplainNode>) -> Vec<String> {
    let mut context = ExplainContext::default();
    let root = explainable.explain(&mut context);
    let root = context.with_subqueries(root);
    let mut lines = Vec::new();
    render_root(&root, &mut lines);
    lines
}

fn render_root(node: &ExplainNode, lines: &mut Vec<String>) {
    lines.push(format!("• {}", node.title()));

    if node.children.is_empty() {
        lines.extend(
            node.properties
                .iter()
                .map(|property| format!("  {}: {}", property.key, property.value)),
        );
        return;
    }

    lines.extend(
        node.properties
            .iter()
            .map(|property| format!("│ {}: {}", property.key, property.value)),
    );
    if !node.properties.is_empty() {
        lines.push("│".to_owned());
    }
    render_children(node, "", lines);
}

fn render_children(node: &ExplainNode, prefix: &str, lines: &mut Vec<String>) {
    for (index, child) in node.children.iter().enumerate() {
        let last = index + 1 == node.children.len();
        render_child(child, prefix, last, lines);
        if !last {
            lines.push(format!("{prefix}│"));
        }
    }
}

fn render_child(node: &ExplainNode, prefix: &str, last: bool, lines: &mut Vec<String>) {
    let branch = if last { "└─ •" } else { "├─ •" };
    let continuation = if last { "   " } else { "│  " };
    let child_prefix = format!("{prefix}{continuation}");

    lines.push(format!("{prefix}{branch} {}", node.title()));

    if node.children.is_empty() {
        lines.extend(
            node.properties
                .iter()
                .map(|property| format!("{child_prefix}  {}: {}", property.key, property.value)),
        );
        return;
    }

    lines.extend(
        node.properties
            .iter()
            .map(|property| format!("{child_prefix}│ {}: {}", property.key, property.value)),
    );
    if !node.properties.is_empty() {
        lines.push(format!("{child_prefix}│"));
    }
    render_children(node, &child_prefix, lines);
}

impl ExplainNode {
    fn title(&self) -> String {
        match &self.annotation {
            Some(annotation) => format!("{} ({annotation})", self.name),
            None => self.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{Explain, ExplainContext, ExplainNode, explain_lines},
        crate::{
            executor::{Payload, execute},
            mock::run,
            parse_sql::parse,
            plan::StatementPlan,
            store::Planner,
            translate::translate,
        },
    };

    struct TestPlan;

    impl Explain for TestPlan {
        type Output = ExplainNode;

        fn explain(&self, _context: &mut ExplainContext) -> ExplainNode {
            ExplainNode::new("root")
                .with_property("root property", 1)
                .with_children([
                    ExplainNode::new("first").with_property("leaf property", 2),
                    ExplainNode::new("second").with_child(ExplainNode::new("nested leaf")),
                ])
        }
    }

    fn explain_sql(setup: &str, sql: &str) -> String {
        let mut storage = run(setup);
        let parsed = parse(sql).unwrap();
        let statement = StatementPlan::from(translate(&parsed[0]).unwrap());
        let planned = storage.plan(statement).unwrap();
        let Payload::Explain(lines) = execute(&mut storage, &planned).unwrap() else {
            panic!("expected explain payload");
        };

        lines.join("\n")
    }

    #[test]
    fn renders_compact_cockroach_style_tree() {
        assert_eq!(
            explain_lines(&TestPlan).join("\n"),
            r"
• root
│ root property: 1
│
├─ • first
│    leaf property: 2
│
└─ • second
   └─ • nested leaf
"
            .trim()
        );
    }

    #[test]
    fn explains_joined_aggregation_pipeline() {
        let actual = explain_sql(
            r"
CREATE TABLE Player (id INT, team_id INT, active BOOLEAN);
CREATE TABLE Badge (player_id INT);
CREATE TABLE Team (id INT);
",
            r"
EXPLAIN
SELECT Player.team_id, COUNT(*) AS player_count
FROM Player
INNER JOIN Badge ON Player.id = Badge.player_id
LEFT JOIN Team ON Player.team_id = Team.id
WHERE Player.active = TRUE
GROUP BY Player.team_id
ORDER BY player_count DESC
LIMIT 10 OFFSET 5
",
        );
        let expected = r"
• limit
│ count: 10
│
└─ • offset
   │ count: 5
   │
   └─ • sort
      │ order: player_count DESC
      │
      └─ • project
         │ columns: Player.team_id, COUNT(*) AS player_count
         │
         └─ • aggregate
            │ group by: Player.team_id
            │ aggregates: COUNT(*)
            │
            └─ • filter
               │ expression: Player.active = TRUE
               │
               └─ • hash join (left outer)
                  │ equality: Player.team_id = Team.id
                  │
                  ├─ • hash join (inner)
                  │  │ equality: Player.id = Badge.player_id
                  │  │
                  │  ├─ • scan Player
                  │  │    access: full scan
                  │  │
                  │  └─ • scan Badge
                  │       access: full scan
                  │
                  └─ • scan Team
                       access: full scan
"
        .trim();
        assert_eq!(actual, expected);
    }

    #[test]
    fn explains_primary_key_access_path() {
        assert_eq!(
            explain_sql(
                "CREATE TABLE Player (id INT PRIMARY KEY, name TEXT);",
                "EXPLAIN SELECT name FROM Player WHERE id = 1",
            ),
            r"
• project
│ columns: name
│
└─ • scan Player
     access: primary key
     key: 1
"
            .trim()
        );
    }

    #[test]
    fn explains_expression_subqueries_as_referenced_plans() {
        assert_eq!(
            explain_sql(
                r"
CREATE TABLE Player (id INT);
CREATE TABLE Badge (player_id INT);
",
                r"
EXPLAIN
SELECT id, (SELECT COUNT(*) AS total FROM Badge) AS badge_count
FROM Player
WHERE id IN (SELECT player_id FROM Badge)
AND EXISTS (
    SELECT *
    FROM Badge
    WHERE Badge.player_id = Player.id
)
",
            ),
            r"
• root
├─ • project
│  │ columns: id, @S1 AS badge_count
│  │
│  └─ • filter
│     │ expression: id IN (@S2) AND EXISTS (@S3)
│     │
│     └─ • scan Player
│          access: full scan
│
├─ • subquery
│  │ id: @S1
│  │ exec mode: one row
│  │
│  └─ • project
│     │ columns: COUNT(*) AS total
│     │
│     └─ • aggregate
│        │ aggregates: COUNT(*)
│        │
│        └─ • scan Badge
│             access: full scan
│
├─ • subquery
│  │ id: @S2
│  │ exec mode: all rows
│  │
│  └─ • project
│     │ columns: player_id
│     │
│     └─ • scan Badge
│          access: full scan
│
└─ • subquery
   │ id: @S3
   │ exec mode: exists
   │
   └─ • project
      │ columns: *
      │
      └─ • filter
         │ expression: Badge.player_id = Player.id
         │
         └─ • scan Badge
              access: full scan
"
            .trim()
        );
    }

    #[test]
    fn explains_distinct_grouping_and_having() {
        assert_eq!(
            explain_sql(
                "CREATE TABLE Item (category TEXT);",
                r"
EXPLAIN
SELECT DISTINCT category, COUNT(*) AS total
FROM Item
GROUP BY category
HAVING COUNT(*) > 1
ORDER BY total DESC
",
            ),
            r"
• distinct
└─ • sort
   │ order: total DESC
   │
   └─ • project
      │ columns: category, COUNT(*) AS total
      │
      └─ • having
         │ expression: COUNT(*) > 1
         │
         └─ • aggregate
            │ group by: category
            │ aggregates: COUNT(*)
            │
            └─ • scan Item
                 access: full scan
"
            .trim()
        );
    }

    #[test]
    fn explains_derived_query() {
        assert_eq!(
            explain_sql(
                "CREATE TABLE Player (id INT);",
                r"
EXPLAIN
SELECT recent.id
FROM (SELECT id FROM Player LIMIT 2) AS recent
",
            ),
            r"
• project
│ columns: recent.id
│
└─ • derived recent
   └─ • limit
      │ count: 2
      │
      └─ • project
         │ columns: id
         │
         └─ • scan Player
              access: full scan
"
            .trim()
        );
    }

    #[test]
    fn explains_values_query() {
        assert_eq!(
            explain_sql(
                "",
                r"
EXPLAIN
VALUES (1, 'a'), (2, 'b')
ORDER BY 1 DESC
LIMIT 1 OFFSET 1
",
            ),
            r"
• limit
│ count: 1
│
└─ • offset
   │ count: 1
   │
   └─ • sort
      │ order: 1 DESC
      │
      └─ • values
           size: 2 columns, 2 rows
"
            .trim()
        );
    }

    #[test]
    fn explains_subquery_in_values() {
        assert_eq!(
            explain_sql(
                "CREATE TABLE Player (id INT);",
                "EXPLAIN VALUES ((SELECT id FROM Player LIMIT 1))",
            ),
            r"
• root
├─ • values
│    size: 1 columns, 1 rows
│    expressions: (@S1)
│
└─ • subquery
   │ id: @S1
   │ exec mode: one row
   │
   └─ • limit
      │ count: 1
      │
      └─ • project
         │ columns: id
         │
         └─ • scan Player
              access: full scan
"
            .trim()
        );
    }

    #[test]
    fn explains_series_source() {
        assert_eq!(
            explain_sql("", "EXPLAIN SELECT * FROM SERIES(3) AS numbers"),
            r"
• project
│ columns: *
│
└─ • series numbers
     size: 3
"
            .trim()
        );
    }

    #[test]
    fn explains_dictionary_source() {
        assert_eq!(
            explain_sql("", "EXPLAIN SELECT * FROM GLUE_TABLES"),
            r"
• project
│ columns: *
│
└─ • dictionary GLUE_TABLES
     source: GLUE_TABLES
"
            .trim()
        );
    }
}
