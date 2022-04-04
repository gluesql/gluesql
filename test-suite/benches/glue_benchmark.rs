use {
	criterion::*,
	gluesql::prelude::*,
	std::time::Duration,
	sled::IVec,
};

fn setup_glue() -> Glue<IVec, SledStorage> {
	let path = "data/sled_bench";

	match std::fs::remove_dir_all(&path) {
		Ok(()) => (),
		Err(e) => {
			println!("fs::remove_file {:?}", e);
		}
	}

	let storage = SledStorage::new(&path)
		.expect("Create Storage");

	Glue::new(storage)
}

fn setup_a(glue: &mut Glue<IVec, SledStorage>) {
	glue.execute(
		"
		CREATE TABLE A (
			pk INTEGER PRIMARY KEY
		)
	",
	)
	.unwrap();
	glue.execute(
		"
		CREATE INDEX primkey ON A (pk)
	",
	)
	.unwrap();
	(0..10_000).into_iter().for_each(|pk| {glue.execute(&format!("INSERT INTO A VALUES ({pk})", pk=pk)).unwrap();});
}

fn setup_b(glue: &mut Glue<IVec, SledStorage>) {
	glue.execute(
		"
		CREATE TABLE B (
			pk INTEGER AUTO_INCREMENT PRIMARY KEY,
			fk INTEGER,
			val FLOAT
		)
	",
	)
	.unwrap();
	glue.execute(
		"
		CREATE INDEX primkey ON B (pk)
	",
	)
	.unwrap();
	(0..100_000).into_iter().for_each(|_| {glue.execute(&format!("INSERT INTO B VALUES ({fk}, {val})", fk=fastrand::i64(0..10_000), val=fastrand::f64())).unwrap();});
}

fn setup_c(glue: &mut Glue<IVec, SledStorage>) {
	glue.execute(
		"
		CREATE TABLE C (
			pk INTEGER AUTO_INCREMENT PRIMARY KEY,
			fk INTEGER,
			val FLOAT
		)
	",
	)
	.unwrap();
	(0..100_000).into_iter().for_each(|_| {glue.execute(&format!("INSERT INTO C VALUES ({fk}, {val})", fk=fastrand::i64(0..10_000), val=fastrand::f64())).unwrap();});
}

fn setup() -> Glue<IVec, SledStorage> {
	let mut glue = setup_glue();
	setup_a(&mut glue);
	setup_b(&mut glue);
	setup_c(&mut glue);
	glue
}

fn filter(table: &str) -> String {
	format!(
		"
		SELECT
			*
		FROM
			{}
		WHERE
			pk < 100
	",
		table
	)
}
fn find(table: &str) -> String {
	format!(
		"
		SELECT
			*
		FROM
			{}
		WHERE
			pk = 100
	",
		table
	)
}
fn sum_group(table: &str) -> String {
	format!(
		"
		SELECT
			SUM(val)
		FROM
			{}
		GROUP BY
			fk
	",
		table
	)
}
fn join(table: &str) -> String {
	format!(
		"
		SELECT
			SUM(val)
		FROM
			A
			INNER JOIN {table}
				ON {table}.fk = A.pk
		GROUP BY
			A.pk
	",
		table = table
	)
}

fn bench(criterion: &mut Criterion) {
	let mut glue = setup();

	let mut group = criterion.benchmark_group("filter");
	group.bench_function("a", |benchmarker| {
		benchmarker.iter(|| glue.execute(&filter("A")).unwrap());
	});
	group.bench_function("b", |benchmarker| {
		benchmarker.iter(|| glue.execute(&filter("B")).unwrap());
	});
	group.bench_function("c", |benchmarker| {
		benchmarker.iter(|| glue.execute(&filter("C")).unwrap());
	});
	group.finish();

	let mut group = criterion.benchmark_group("find");
	group.bench_function("a", |benchmarker| {
		benchmarker.iter(|| glue.execute(&find("A")).unwrap());
	});
	group.bench_function("b", |benchmarker| {
		benchmarker.iter(|| glue.execute(&find("B")).unwrap());
	});
	group.bench_function("c", |benchmarker| {
		benchmarker.iter(|| glue.execute(&find("C")).unwrap());
	});
	group.finish();

	let mut group = criterion.benchmark_group("sum_group");
	group
		.sampling_mode(SamplingMode::Flat)
		.measurement_time(Duration::from_secs(20));
	group.bench_function("b", |benchmarker| {
		benchmarker.iter(|| glue.execute(&sum_group("B")).unwrap());
	});
	group.bench_function("c", |benchmarker| {
		benchmarker.iter(|| glue.execute(&sum_group("C")).unwrap());
	});
	group.finish();

	let mut group = criterion.benchmark_group("join");
	group
		.sampling_mode(SamplingMode::Flat)
		.measurement_time(Duration::from_secs(30));
	group.bench_function("b", |benchmarker| {
		benchmarker.iter(|| glue.execute(&join("B")).unwrap());
	});
	group.bench_function("c", |benchmarker| {
		benchmarker.iter(|| glue.execute(&join("C")).unwrap());
	});
	group.finish();
}

criterion_group! {
	name = benches;
	config = Criterion::default().noise_threshold(0.05).sample_size(10).warm_up_time(Duration::from_secs(5)).measurement_time(Duration::from_secs(10));
	targets = bench
}
criterion_main!(benches);
