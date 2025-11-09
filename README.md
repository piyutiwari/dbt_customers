# dbt PostgreSQL Medallion Architecture

A dbt (data build tool) project implementing the medallion architecture (Bronze, Silver, Gold) with PostgreSQL as the data warehouse.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Data Models](#data-models)
- [Testing](#testing)
- [Troubleshooting](#troubleshooting)

## Overview

This project demonstrates a modern data pipeline using dbt and PostgreSQL, organized using the medallion architecture pattern:

- **Bronze Layer**: Raw data ingestion with minimal transformation
- **Silver Layer**: Cleaned, validated, and standardized data
- **Gold Layer**: Business-level aggregations and metrics

## Architecture

```
┌─────────────────┐
│   Raw Sources   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Bronze Layer   │  ← Raw data with metadata
│   (Views)       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Silver Layer   │  ← Cleaned & validated data
│   (Tables)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Gold Layer    │  ← Business metrics & KPIs
│   (Tables)      │
└─────────────────┘
```

## Prerequisites

Before setting up this project, ensure you have the following installed:

### Required Software

1. **Python 3.9+**
   - Check version: `python --version`
   - Download: https://www.python.org/downloads/

2. **PostgreSQL**
   - Check if installed: `psql --version`

   **Installation Instructions:**

   **Windows:**
   ```bash
   # Download from official site
   # Visit: https://www.postgresql.org/download/windows/
   # Or use Chocolatey:
   choco install postgresql
   ```

   **macOS:**
   ```bash
   # Using Homebrew
   brew install postgresql@15
   brew services start postgresql@15
   ```

   **Linux (Ubuntu/Debian):**
   ```bash
   sudo apt update
   sudo apt install postgresql postgresql-contrib
   sudo systemctl start postgresql
   sudo systemctl enable postgresql
   ```

3. **uv (Python package manager)**
   - Check if installed: `uv --version`

   **Installation:**
   ```bash
   # Windows (PowerShell)
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

   # macOS/Linux
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

## Installation

### 1. Clone the Repository

```bash
git clone <your-repo-url>
cd repo
```

### 2. Install Dependencies with uv

```bash
# Install project dependencies
uv sync

# Activate the virtual environment
# On Windows:
.venv\Scripts\activate

# On macOS/Linux:
source .venv/bin/activate
```

### 3. Set Up PostgreSQL Database

```bash
# Connect to PostgreSQL
psql -U postgres

# Create database
CREATE DATABASE medallion_db;

# Create schemas
\c medallion_db
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
CREATE SCHEMA snapshots;

# Exit psql
\q
```

### 4. Configure Environment Variables

```bash
# Copy the example environment file
cp .env.example .env

# Edit .env with your PostgreSQL credentials
# Default values:
# POSTGRES_USER=postgres
# POSTGRES_PASSWORD=postgres
# POSTGRES_DB=medallion_db
# POSTGRES_HOST=localhost
# POSTGRES_PORT=5432
```

### 5. Test dbt Connection

```bash
cd dbt_project

# Test connection
dbt debug --profiles-dir .

# You should see "All checks passed!"
```

## Configuration

### profiles.yml

The `dbt_project/profiles.yml` file contains database connection settings. It uses environment variables for security:

```yaml
postgres_medallion:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5432
      user: "{{ env_var('POSTGRES_USER', 'postgres') }}"
      password: "{{ env_var('POSTGRES_PASSWORD', 'postgres') }}"
      dbname: "{{ env_var('POSTGRES_DB', 'medallion_db') }}"
      schema: public
      threads: 4
```

### dbt_project.yml

The `dbt_project/dbt_project.yml` file configures the project structure and materializations:

- Bronze models: Materialized as **views**
- Silver models: Materialized as **tables**
- Gold models: Materialized as **tables**

## Usage

### Load Sample Data

```bash
cd dbt_project

# Load seed files (sample data)
dbt seed --profiles-dir .
```

This creates the `raw_customers` and `raw_orders` tables in your database with sample data.

### Run dbt Models

```bash
# Run all models
dbt run --profiles-dir .

# Run specific layer
dbt run --profiles-dir . --select bronze.*
dbt run --profiles-dir . --select silver.*
dbt run --profiles-dir . --select gold.*

# Run specific model
dbt run --profiles-dir . --select bronze_customers
```

### Run Tests

```bash
# Run all tests
dbt test --profiles-dir .

# Run tests for specific model
dbt test --profiles-dir . --select silver_customers
```

### Generate Documentation

```bash
# Generate documentation
dbt docs generate --profiles-dir .

# Serve documentation
dbt docs serve --profiles-dir .
```

## Project Structure

```
repo/
├── dbt_project/
│   ├── models/
│   │   ├── bronze/              # Raw data layer
│   │   │   ├── bronze_customers.sql
│   │   │   ├── bronze_orders.sql
│   │   │   └── schema.yml
│   │   ├── silver/              # Cleaned data layer
│   │   │   ├── silver_customers.sql
│   │   │   ├── silver_orders.sql
│   │   │   └── schema.yml
│   │   └── gold/                # Aggregated metrics layer
│   │       ├── gold_customer_metrics.sql
│   │       ├── gold_daily_revenue.sql
│   │       └── schema.yml
│   ├── macros/                  # Reusable SQL functions
│   │   └── generate_schema_name.sql
│   ├── seeds/                   # Sample CSV data
│   │   ├── sample_raw_customers.csv
│   │   └── sample_raw_orders.csv
│   ├── tests/                   # Custom data tests
│   ├── snapshots/               # SCD Type 2 snapshots
│   ├── dbt_project.yml          # dbt project configuration
│   ├── profiles.yml             # Database connection config
│   └── packages.yml             # dbt package dependencies
├── scripts/                     # Setup scripts
│   └── setup_database.sql       # PostgreSQL database setup
├── .env.example                 # Environment variables template
├── .gitignore                   # Git ignore rules
├── pyproject.toml               # Python dependencies (uv)
├── justfile                     # Task runner commands
├── QUICKSTART.md                # Quick start guide
├── CONTRIBUTING.md              # Contributing guidelines
└── README.md                    # This file
```

## Data Models

### Bronze Layer (Raw Data)

**Purpose**: Ingest raw data with minimal transformation

- `bronze_customers`: Raw customer data
- `bronze_orders`: Raw order data

**Materialization**: Views (for efficiency and freshness)

### Silver Layer (Cleaned Data)

**Purpose**: Apply data quality rules, standardization, and validation

- `silver_customers`: Cleaned customer data with standardized names and emails
- `silver_orders`: Validated orders with business rules applied

**Materialization**: Tables (for performance)

**Transformations**:
- Data cleansing (trim, case standardization)
- Email validation
- Data type conversions
- Null handling
- Business rule validation

### Gold Layer (Business Metrics)

**Purpose**: Provide business-ready analytics and KPIs

- `gold_customer_metrics`: Customer-level aggregations (lifetime value, order counts, etc.)
- `gold_daily_revenue`: Daily revenue metrics and trends

**Materialization**: Tables (optimized for BI tools)

**Metrics**:
- Customer lifetime value
- Average order value
- Revenue aggregations
- Customer segmentation
- Time-series metrics

## Testing

### Data Quality Tests

The project includes various tests defined in `schema.yml` files:

1. **Uniqueness Tests**: Ensure primary keys are unique
2. **Not Null Tests**: Validate required fields
3. **Relationship Tests**: Check referential integrity
4. **Custom Tests**: Business-specific validations

### Running Tests

```bash
# Run all tests
dbt test --profiles-dir .

# Run tests for specific layer
dbt test --profiles-dir . --select bronze.*

# Run specific test type
dbt test --profiles-dir . --select test_type:unique
dbt test --profiles-dir . --select test_type:not_null
```

## Troubleshooting

### PostgreSQL Connection Issues

**Error**: "connection refused"
```bash
# Check if PostgreSQL is running
# Windows:
pg_ctl status

# macOS/Linux:
sudo systemctl status postgresql
```

**Error**: "role does not exist"
```bash
# Create PostgreSQL user
createuser -s postgres
```

**Error**: "database does not exist"
```bash
# Create database
createdb medallion_db
```

### dbt Issues

**Error**: "Could not find profile"
```bash
# Ensure you're running dbt from the correct directory
cd dbt_project
dbt debug --profiles-dir .
```

**Error**: "Compilation Error"
```bash
# Check model SQL syntax
dbt compile --profiles-dir .
```

### uv Issues

**Error**: "uv: command not found"
```bash
# Reinstall uv
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or on Windows PowerShell:
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Restart your terminal after installation
```

## Using Just Commands

This project uses [just](https://github.com/casey/just) as a command runner for common tasks.

### Install just

**Windows:**
1. Download from: https://github.com/casey/just/releases
2. Extract `just.exe` and add to PATH
3. Or use: `winget install Casey.Just` or `choco install just` or `scoop install just`

**macOS:**
```bash
brew install just
```

**Linux:**
```bash
curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin
```

**Using Cargo (any platform):**
```bash
cargo install just
```

### Available Commands

```bash
# List all available commands
just

# Install dependencies
just install

# Set up database
just setup-db

# Set up environment file
just setup-env

# Complete setup (install, setup-db, seed, run, test)
just setup-all

# Run the complete pipeline
just pipeline

# Load sample data
just seed

# Run all models
just run

# Run specific layer
just run-bronze
just run-silver
just run-gold

# Run specific model
just run-model bronze_customers

# Run tests
just test

# Test specific layer
just test-layer silver

# Test specific model
just test-model silver_customers

# Generate and serve documentation
just docs

# Clean artifacts
just clean

# Check prerequisites
just check

# Show project info
just info
```

## Development Workflow

1. **Make changes** to models or add new models
2. **Run models**: `just run-model <model_name>` or `dbt run --profiles-dir . --select <model_name>`
3. **Test changes**: `just test-model <model_name>` or `dbt test --profiles-dir . --select <model_name>`
4. **Document**: Update `schema.yml` files with descriptions
5. **Generate docs**: `just docs` or `dbt docs generate --profiles-dir .`
6. **Commit**: Commit your changes to version control

## Additional Resources

- [dbt Documentation](https://docs.getdbt.com/)
- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [uv Documentation](https://docs.astral.sh/uv/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

## License

MIT License - feel free to use this project as a template for your own dbt projects.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
