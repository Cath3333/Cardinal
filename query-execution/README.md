# Cardinal Query Execution Environment

PostgreSQL query execution and benchmarking environment for the Cardinal query optimization project. This setup allows you to run SQL queries, collect execution plans, measure performance, and experiment with PostgreSQL hints.

## Project Structure

```
cardinal/query-execution/
├── .env                     # Database credentials (create this)
├── .gitignore              # Git ignore file
├── requirements.txt        # Python dependencies
├── config.py              # Configuration loader
├── setup_db.py            # Database setup script
├── query_executor.py      # Simple query executor
├── executor_cli.py # Interactive CLI tool
├── data/                  # Directory for data files
├── results/               # Directory for results
└── logs/                  # Directory for log files
```

## Prerequisites

- PostgreSQL 12+ installed
- Python 3 installed

## Installation

### 1. Install PostgreSQL

```bash
brew install postgresql@14
brew services start postgresql@14
echo 'export PATH="/opt/homebrew/opt/postgresql@14/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
```

### 2. Create PostgreSQL User

```bash
# Method 1: Using createuser command
createuser --interactive --pwprompt your_username

# Method 2: Using psql
psql postgres
CREATE USER your_username WITH PASSWORD 'your_password' SUPERUSER;
\q
```

### 3. Install Python Dependencies

```bash
pip install psycopg2-binary python-dotenv pandas numpy
# OR
pip install -r requirements.txt
```

### 4. Create Environment File

Create a `.env` file in the project root with your PostgreSQL credentials:

```bash
# .env
POSTGRES_HOST=localhost
POSTGRES_DB=cardinal_test
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_actual_password
POSTGRES_PORT=5432
```

**Important:** 
- Replace `your_username` and `your_actual_password` with your actual credentials
- Do not include quotes, backslashes, or trailing spaces
- Add `.env` to your `.gitignore` to keep credentials secure

### 5. Create Project Directories

```bash
mkdir -p data results logs
```

### 6. Set Up Database

```bash
python setup_db.py
```

This creates the `cardinal_test` database with sample tables:
- `customers` (customer information)
- `orders` (order records)
- `order_items` (order line items)

## Usage

### Basic Query Execution

```bash
# Test simple executor
python query_executor.py
```

### Interactive Query Tool

**Interactive Mode:**
```bash
python executor_cli.py
```

**Single Query Mode:**
```bash
# Basic query
python executor_cli.py -q "SELECT * FROM customers WHERE country = 'USA'"

# With PostgreSQL hints
python executor_cli.py -q "SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id" --hints "/*+ HashJoin(c o) */"

# Custom benchmark iterations
python executor_cli.py -q "SELECT COUNT(*) FROM customers" -i 10

# Verbose mode (shows full execution plan)
python executor_cli.py -q "SELECT * FROM customers" --verbose
```

### Example Queries to Test

```sql
-- Simple selection
SELECT * FROM customers WHERE country = 'USA';

-- Join query
SELECT c.name, COUNT(o.order_id) as order_count
FROM customers c 
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name;

-- Complex join
SELECT c.name, oi.product_name, SUM(oi.quantity) as total_quantity
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
WHERE o.status = 'completed'
GROUP BY c.name, oi.product_name
ORDER BY total_quantity DESC;
```

### PostgreSQL Hints

The executor supports PostgreSQL hints via the `pg_hint_plan` extension:

```sql
/*+ HashJoin(customers orders) SeqScan(customers) */
SELECT * FROM customers c JOIN orders o ON c.customer_id = o.customer_id;
```

**Common hint types:**
- Join methods: `HashJoin(a b)`, `NestLoop(a b)`, `MergeJoin(a b)`
- Scan methods: `SeqScan(table)`, `IndexScan(table)`

## Troubleshooting

### Connection Issues

**Error: "psql: command not found"**
- PostgreSQL not in PATH
- Add PostgreSQL bin directory to your PATH

**Error: "connection refused"**
- PostgreSQL service not running
- Start with `brew services start postgresql@14` (macOS) or `sudo systemctl start postgresql` (Linux)

**Error: "password authentication failed"**
- Check credentials in `.env` file match your PostgreSQL user
- Ensure no extra characters or spaces in `.env`

**Error: "role does not exist"**
- PostgreSQL user not created
- Create user with `createuser` or `CREATE USER` command

### Database Issues

**Error: "database does not exist"**
- Run `python setup_db.py` to create test database
- Or connect to default `postgres` database first

**Error: "relation does not exist"**
- Sample tables not created
- Check if `setup_db.py` completed successfully

### Python Issues

**Error: "command not found: python"**
- Use `python3` instead of `python` on some systems
- Or create alias: `alias python=python3`

**Error: "No module named 'psycopg2'"**
- Install dependencies: `pip install psycopg2-binary`

## Verification Commands

Test your setup:

```bash
# Check PostgreSQL is running
brew services list | grep postgresql  # macOS
sudo systemctl status postgresql      # Linux

# Test connection
psql -U your_username -d postgres -h localhost -c "SELECT version();"

# Check test database
psql -U your_username -d cardinal_test -h localhost -c "\dt"

# Test Python executor
python3 executor_cli.py -q "SELECT version();"
```

## Optional: pg_hint_plan Extension

For advanced hint functionality:

**Installation (varies by system):**
```bash
# Ubuntu/Debian
sudo apt-get install postgresql-14-pg-hint-plan

# macOS - pg_hint_plan is not available via Homebrew
# You may need to compile from source or use PostgreSQL.app
# For initial setup, you can skip this extension
```

**Enable in database:**
```bash
psql -U your_username -d cardinal_test -h localhost
CREATE EXTENSION pg_hint_plan;
\q
```

**Note:** If pg_hint_plan is not available on your system, the query executor will still work normally but hint functionality will be disabled. This is sufficient for initial development and testing.

## Files Description

**Configuration:**
- `config.py` - Loads environment variables and configuration
- `.env` - Database credentials (you create this)

**Database Setup:**
- `setup_db.py` - Creates database and sample tables

**Query Execution:**
- `query_executor.py` - Core query execution classes and functions
- `executor_cli.py` - Command-line interface for query testing

## Next Steps

Once setup is complete, you can:

1. Test basic query execution and plan collection
2. Experiment with different PostgreSQL hints
3. Benchmark query performance across different strategies
4. Prepare for LLM-based query optimization experiments

This environment provides the foundation for collecting the `{query + schema, execution plan}` pairs needed for the Cardinal training pipeline.

