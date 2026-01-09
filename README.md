# Terraform Provider for pgmold

Terraform provider for [pgmold](https://github.com/fmguerreiro/pgmold) - declarative PostgreSQL schema management.

## Features

- **Declarative schema management**: Define your PostgreSQL schema in SQL files and let pgmold handle migrations
- **Automatic diff detection**: Compares schema file against live database and generates migrations
- **Safe migrations**: Built-in linting to prevent destructive operations without explicit approval

## Installation

```hcl
terraform {
  required_providers {
    pgmold = {
      source  = "fmguerreiro/pgmold"
      version = "~> 0.1"
    }
  }
}
```

## Usage

### pgmold_schema

Applies a schema file to a PostgreSQL database:

```hcl
resource "pgmold_schema" "app" {
  schema_file       = "${path.module}/schema.sql"
  database_url      = "postgres://user:pass@localhost:5432/mydb"
  allow_destructive = false  # Set to true to allow DROP operations
}
```

### pgmold_migration

Generates numbered migration files instead of applying directly:

```hcl
resource "pgmold_migration" "app" {
  schema_file  = "${path.module}/schema.sql"
  database_url = "postgres://user:pass@localhost:5432/mydb"
  output_dir   = "${path.module}/migrations"
  prefix       = "V"  # Optional: Flyway-style prefix
}
```

## Attributes

### pgmold_schema

| Name | Type | Required | Description |
|------|------|----------|-------------|
| schema_file | string | yes | Path to SQL schema file |
| database_url | string | yes | PostgreSQL connection URL |
| target_schemas | list(string) | no | PostgreSQL schemas to manage (default: ["public"]) |
| allow_destructive | bool | no | Allow DROP operations (default: false) |
| zero_downtime | bool | no | Use expand/contract pattern (default: false) |

**Computed attributes:**
- `id` - Resource identifier
- `schema_hash` - SHA256 hash of schema file
- `applied_at` - Timestamp of last migration
- `migration_count` - Number of operations applied

### pgmold_migration

| Name | Type | Required | Description |
|------|------|----------|-------------|
| schema_file | string | yes | Path to SQL schema file |
| database_url | string | yes | PostgreSQL connection URL |
| output_dir | string | yes | Directory to write migration files |
| prefix | string | no | Optional prefix like 'V' for Flyway |
| target_schemas | list(string) | no | PostgreSQL schemas to introspect |

**Computed attributes:**
- `id` - Resource identifier
- `schema_hash` - SHA256 hash of schema file
- `migration_file` - Path to generated migration file
- `migration_number` - Auto-incremented migration number
- `operations` - List of migration operations

## License

MIT
