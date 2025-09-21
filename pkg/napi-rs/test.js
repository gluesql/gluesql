const { Glue } = require('./index.js');

async function test() {
    try {
        console.log('Creating GlueSQL instance...');
        const db = new Glue();

        console.log('Running test query...');
        const result = await db.query(`
            CREATE TABLE test (id INTEGER, name TEXT);
            INSERT INTO test VALUES (1, 'hello'), (2, 'world');
            SELECT * FROM test;
        `);

        console.log('Query result:', JSON.stringify(result, null, 2));
        console.log('Test completed successfully!');
    } catch (error) {
        console.error('Test failed:', error);
        process.exit(1);
    }
}

test();
