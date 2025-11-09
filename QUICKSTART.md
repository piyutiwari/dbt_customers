# Quick Start Guide

Get up and running with the dbt PostgreSQL Medallion project in minutes!

## Prerequisites Check

```bash
# Check Python
python --version  # Should be 3.9+

# Check PostgreSQL
psql --version

# Check uv
uv --version
```

## 5-Minute Setup

### Step 1: Install Dependencies

```bash
# Install uv if not already installed
# Windows (PowerShell):
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# macOS/Linux:
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install project dependencies
uv sync
```

### Step 2: Set Up PostgreSQL

```bash
# Run the database setup script
psql -U postgres -f scripts/setup_database.sql

# Or manually create database:
psql -U postgres
CREATE DATABASE medallion_db;
\c medallion_db
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA snapshots;
\q
```

### Step 3: Configure Environment

```bash
# Copy environment template
cp .env.example .env

# Edit .env with your credentials (optional if using defaults)
# Default values work if you're using standard PostgreSQL setup
```

### Step 4: Test Connection

```bash
cd dbt_project
dbt debug --profiles-dir .
```

You should see: **"All checks passed!"**

### Step 5: Load Sample Data and Run Models

```bash
# Load sample data
dbt seed --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .
```

### Step 6: View Results

```bash
# Generate and view documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Open your browser to http://localhost:8080 to explore the data lineage and documentation!

## Verify Everything Works

Connect to PostgreSQL and check the created tables:

```bash
psql -U postgres -d medallion_db

# List all tables
\dt bronze.*
\dt silver.*
\dt gold.*

# Query gold layer metrics
SELECT * FROM gold.gold_customer_metrics;
SELECT * FROM gold.gold_daily_revenue;

\q
```

## Next Steps

- Explore the models in `dbt_project/models/`
- Modify the sample data in `dbt_project/seeds/`
- Add your own data sources
- Create custom models for your use case

## Common Issues

**Issue**: "connection refused"
- **Solution**: Make sure PostgreSQL is running: `pg_ctl status`

**Issue**: "role does not exist"
- **Solution**: Create the user: `createuser -s postgres`

**Issue**: "database does not exist"
- **Solution**: Run the setup script or create manually

## Using Just Commands (Recommended)

This project uses `just` as a command runner. Install it for easier workflow:

**Windows:**
- Download from: https://github.com/casey/just/releases
- Extract `just.exe` and add to PATH
- Or use: `winget install Casey.Just` or `choco install just`

**macOS:**
```bash
brew install just
```

**Linux:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
```

Then use these commands:

```bash
just install      # Install dependencies
just setup-all    # Complete setup (all steps)
just seed         # Load sample data
just run          # Run all models
just test         # Run tests
just docs         # Generate and serve docs
just check        # Check prerequisites
just              # List all commands
```

## That's It!

You now have a fully functional dbt project with medallion architecture running on PostgreSQL!

For more details, see the full [README.md](README.md).
