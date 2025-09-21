const { Glue } = require('../../index.js');

async function complexExample() {
  console.log('🔧 Running Complex SQL Operations Example');

  const db = new Glue();

  console.log('\n📊 Creating e-commerce schema...');
  await db.query(`
    CREATE TABLE customers (
      id INTEGER,
      name TEXT,
      email TEXT,
      country TEXT
    );

    CREATE TABLE orders (
      id INTEGER,
      customer_id INTEGER,
      total REAL,
      order_date TEXT
    );

    CREATE TABLE products (
      id INTEGER,
      name TEXT,
      price REAL,
      category TEXT
    );

    CREATE TABLE order_items (
      order_id INTEGER,
      product_id INTEGER,
      quantity INTEGER
    );
  `);

  console.log('📝 Inserting sample data...');
  await db.query(`
    INSERT INTO customers VALUES
      (1, 'Alice Johnson', 'alice@example.com', 'USA'),
      (2, 'Bob Smith', 'bob@example.com', 'Canada'),
      (3, 'Charlie Brown', 'charlie@example.com', 'UK');

    INSERT INTO products VALUES
      (1, 'Laptop', 999.99, 'Electronics'),
      (2, 'Mouse', 29.99, 'Electronics'),
      (3, 'Keyboard', 79.99, 'Electronics'),
      (4, 'Book', 19.99, 'Books');

    INSERT INTO orders VALUES
      (1, 1, 1109.97, '2024-01-15'),
      (2, 2, 49.98, '2024-01-16'),
      (3, 1, 19.99, '2024-01-17');

    INSERT INTO order_items VALUES
      (1, 1, 1),
      (1, 2, 1),
      (1, 3, 1),
      (2, 2, 1),
      (2, 4, 1),
      (3, 4, 1);
  `);

  console.log('\n📊 Running complex queries...');

  // Customer orders summary
  let sql = `
    SELECT
      c.name as customer_name,
      COUNT(o.id) as order_count,
      SUM(o.total) as total_spent
    FROM customers c
    LEFT JOIN orders o ON c.id = o.customer_id
    GROUP BY c.id, c.name
    ORDER BY total_spent DESC
  `;
  console.log('\n[Query] Customer Order Summary:');
  console.log(sql);
  const [customerSummary] = await db.query(sql);
  console.table(customerSummary.rows);

  // Product sales analysis
  sql = `
    SELECT
      p.name as product_name,
      p.category,
      SUM(oi.quantity) as total_sold,
      SUM(oi.quantity * p.price) as revenue
    FROM products p
    JOIN order_items oi ON p.id = oi.product_id
    GROUP BY p.id, p.name, p.category
    ORDER BY revenue DESC
  `;
  console.log('\n[Query] Product Sales Analysis:');
  console.log(sql);
  const [productSales] = await db.query(sql);
  console.table(productSales.rows);

  // Order details with customer info
  sql = `
    SELECT
      o.id as order_id,
      c.name as customer_name,
      o.order_date,
      p.name as product_name,
      oi.quantity,
      (oi.quantity * p.price) as line_total
    FROM orders o
    JOIN customers c ON o.customer_id = c.id
    JOIN order_items oi ON o.id = oi.order_id
    JOIN products p ON oi.product_id = p.id
    ORDER BY o.id, p.name
  `;
  console.log('\n[Query] Detailed Order Information:');
  console.log(sql);
  const [orderDetails] = await db.query(sql);
  console.table(orderDetails.rows);

  console.log('\n✅ Complex example completed successfully!');
}

complexExample().catch(error => {
  console.error('❌ Complex example failed:', error);
  process.exit(1);
});
