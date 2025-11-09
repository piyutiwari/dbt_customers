# justfile for dbt PostgreSQL Medallion Project
# Install just: https://github.com/casey/just

# List all available recipes
default:
    @just --list

# Install dependencies using uv
install:
    uv sync

# Set up PostgreSQL database and schemas
setup-db:
    @echo "Setting up PostgreSQL database..."
    @echo "Running database setup script..."
    psql -U postgres -f scripts/setup_database.sql

# Create .env file from template
setup-env:
    @echo "Creating .env file from template..."
    @if [ ! -f .env ]; then cp .env.example .env && echo ".env created! Please edit with your credentials."; else echo ".env already exists"; fi

# Test dbt connection
debug:
    cd dbt_project && dbt debug --profiles-dir .

# Load sample data into database
seed:
    cd dbt_project && dbt seed --profiles-dir .

# Run all dbt models
run:
    cd dbt_project && dbt run --profiles-dir .

# Run only bronze layer models
run-bronze:
    cd dbt_project && dbt run --profiles-dir . --select bronze.*

# Run only silver layer models
run-silver:
    cd dbt_project && dbt run --profiles-dir . --select silver.*

# Run only gold layer models
run-gold:
    cd dbt_project && dbt run --profiles-dir . --select gold.*

# Run all dbt tests
test:
    cd dbt_project && dbt test --profiles-dir .

# Run tests for specific layer
test-layer layer:
    cd dbt_project && dbt test --profiles-dir . --select {{layer}}.*

# Generate dbt documentation
docs-generate:
    cd dbt_project && dbt docs generate --profiles-dir .

# Serve dbt documentation
docs-serve:
    cd dbt_project && dbt docs serve --profiles-dir .

# Generate and serve documentation
docs: docs-generate docs-serve

# Clean dbt artifacts
clean:
    cd dbt_project && dbt clean --profiles-dir .
    @echo "Removing dbt artifacts..."
    @if [ -d "dbt_project/target" ]; then rm -rf dbt_project/target; fi
    @if [ -d "dbt_project/dbt_packages" ]; then rm -rf dbt_project/dbt_packages; fi
    @if [ -d "dbt_project/logs" ]; then rm -rf dbt_project/logs; fi

# Install dbt packages
deps:
    cd dbt_project && dbt deps --profiles-dir .

# Full setup: install, setup database, and run everything
setup-all: install setup-env setup-db debug seed run test
    @echo ""
    @echo "✓ Full setup complete!"
    @echo "Run 'just docs' to view documentation"

# Run the complete pipeline: seed, run models, and test
pipeline: seed run test
    @echo ""
    @echo "✓ Pipeline complete!"

# Compile dbt models without running them
compile:
    cd dbt_project && dbt compile --profiles-dir .

# Run a specific model
run-model model:
    cd dbt_project && dbt run --profiles-dir . --select {{model}}

# Test a specific model
test-model model:
    cd dbt_project && dbt test --profiles-dir . --select {{model}}

# Show dbt project info
info:
    @echo "Project: dbt PostgreSQL Medallion Architecture"
    @echo "Location: $(pwd)"
    @echo ""
    @echo "Python version:"
    @python --version
    @echo ""
    @echo "PostgreSQL version:"
    @psql --version || echo "PostgreSQL not found"
    @echo ""
    @echo "uv version:"
    @uv --version || echo "uv not found"
    @echo ""
    @echo "dbt version:"
    @cd dbt_project && dbt --version || echo "dbt not found"

# Check if all prerequisites are installed
check:
    @echo "Checking prerequisites..."
    @echo -n "Python: "
    @python --version || echo "✗ Not found"
    @echo -n "PostgreSQL: "
    @psql --version || echo "✗ Not found"
    @echo -n "uv: "
    @uv --version || echo "✗ Not found"
    @echo -n "just: "
    @just --version || echo "✗ Not found"
    @echo ""
    @echo "Run 'just install' to install dependencies"
