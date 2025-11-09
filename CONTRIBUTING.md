# Contributing to dbt PostgreSQL Medallion Project

Thank you for your interest in contributing to this project!

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone <your-fork-url>`
3. Create a feature branch: `git checkout -b feature/your-feature-name`
4. Make your changes
5. Test your changes: `make test`
6. Commit your changes: `git commit -m "Description of changes"`
7. Push to your fork: `git push origin feature/your-feature-name`
8. Create a Pull Request

## Development Guidelines

### Code Style

- Follow SQL style guide for dbt models:
  - Use lowercase for SQL keywords
  - Use snake_case for column and table names
  - Indent with 4 spaces
  - Use CTEs (Common Table Expressions) for complex queries

### Model Structure

- **Bronze Layer**: Minimal transformation, raw data ingestion
- **Silver Layer**: Data cleansing, validation, standardization
- **Gold Layer**: Business metrics and aggregations

### Documentation

- Add descriptions for all models in `schema.yml` files
- Document all columns with clear descriptions
- Add tests for data quality

### Testing

Before submitting a PR:

```bash
# Run all models
just run

# Run all tests
just test

# Generate documentation
just docs

# Or run the complete pipeline
just pipeline
```

## Questions?

Feel free to open an issue for any questions or concerns.
