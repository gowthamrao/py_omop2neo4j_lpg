# omop2neo4j

A robust, high-performance Python package to orchestrate the migration of OMOP vocabulary tables from PostgreSQL to a mature Labeled Property Graph (LPG) in Neo4j.

## Overview

This package provides a suite of command-line tools to manage the ETL process for moving OMOP vocabulary data into a Neo4j graph database. It is designed for performance and scalability, prioritizing the use of native database tools (`COPY` and `LOAD CSV`) and providing two main loading strategies:

1.  **Online Loading (`load-csv`):** An easy-to-use method that streams data directly into a running Neo4j instance using `LOAD CSV`. Ideal for most standard vocabulary sizes and when the Neo4j database is online.
2.  **Offline Loading (`prepare-bulk`):** A high-performance method for extremely large datasets. It prepares data files for Neo4j's offline `neo4j-admin database import` tool, which is significantly faster than `LOAD CSV` for bulk operations.

## Prerequisites

*   Python 3.11+
*   Docker and Docker Compose (recommended for running Neo4j)
*   Access to a PostgreSQL database with the OMOP CDM vocabulary tables.

## 1. Neo4j Setup (Recommended)

The `load-csv` command requires a running Neo4j instance with the **APOC** plugin installed. The easiest way to set this up is with Docker. The command also needs access to the generated CSV files, which requires mounting a local directory to the Neo4j container's `/import` directory.

Here is a reference `docker-compose.yml` file to configure the service correctly:

```yaml
# docker-compose.yml
version: '3.8'
services:
  neo4j:
    image: neo4j:latest
    container_name: neo4j-omop-vocab
    environment:
      - NEO4J_AUTH=neo4j/StrongPass123
      - NEO4J_apoc_import_file_enabled=true
      - NEO4J_apoc_import_file_use__neo4j__config=true
      - NEO4JLABS_PLUGINS=["apoc"]
      # Example Memory Tuning (Adjust based on your hardware)
      # Note the double underscores for nested config keys, as per official docs.
      - NEO4J_server_memory_heap_initial__size=4G
      - NEO4J_server_memory_heap_max__size=4G
      - NEO4J_server_memory_pagecache_size=8G
    ports:
      - "7474:7474"
      - "7687:7687"
    volumes:
      - ./neo4j/data:/data
      # Mounts the local 'export' directory to the container's '/import' directory
      - ./export:/import
```

This repository includes this file, so you can simply run:
```bash
docker-compose up -d
```

## 2. Package Installation and Configuration

1.  **Install Package:**
    It is recommended to install the package in a Python virtual environment.
    ```bash
    # Clone the repository (if you haven't already)
    # git clone <repo_url>
    # cd omop2neo4j

    # Install
    pip install .
    ```

2.  **Configure Environment:**
    The application is configured using environment variables. Create a `.env` file in the root of the project directory by copying the example file:
    ```bash
    cp .env.example .env
    ```
    Now, edit the `.env` file with your specific database credentials. **Make sure the `NEO4J_PASSWORD` is set to `StrongPass123` to match `docker-compose.yml`**.

## 3. Usage (Online `load-csv` Workflow)

The package provides a single command-line interface, `omop2neo4j`.

### Step 1: Extract Data from PostgreSQL

Run the `extract` command to export the necessary vocabulary tables from PostgreSQL into CSV files. The files will be saved in the directory specified by `EXPORT_DIR` (default: `./export`), which is the same directory mounted into the Neo4j container.

```bash
omop2neo4j extract
```

### Step 2: Load Data into Neo4j

Run the `load-csv` command to perform a full reload of the Neo4j database. This single command will automatically:
1.  Clear the entire Neo4j database.
2.  Create the necessary constraints and indexes.
3.  Load all the data from the CSV files.

```bash
omop2neo4j load-csv
```

The process can take several minutes depending on the size of the vocabulary and your hardware. Check the logs for detailed progress.

### Step 3: Validate the Loaded Data

After loading, run the `validate` command to check the integrity of the graph. This command runs a series of checks and prints a JSON report summarizing the results.

```bash
omop2neo4j validate
```

## 4. Usage (Offline `prepare-bulk` Workflow)

This method is recommended for very large vocabularies where the online `load-csv` method may be too slow or memory-intensive for the Neo4j server. This process involves generating formatted CSV files and then using the `neo4j-admin` tool while the database is offline.

### Step 1: Extract Data from PostgreSQL

This step is the same as the online workflow.
```bash
omop2neo4j extract
```

### Step 2: Prepare Data for Bulk Import

Run the `prepare-bulk` command. This will read the CSVs from the `export` directory, process them in chunks, and create a new set of files optimized for the importer in the `bulk_import` directory (or a directory of your choice).

```bash
omop2neo4j prepare-bulk
```
At the end of the process, it will print the exact `neo4j-admin` command you need to run.

### Step 3: Run the Neo4j Admin Import

1.  **Stop the Neo4j Service:**
    ```bash
    docker-compose stop neo4j
    ```
2.  **Run the command:** Execute the exact command that was generated by the `prepare-bulk` step. It must be run on the machine where the `docker-compose.yml` is located. The command will correctly use `docker-compose exec` to run the import inside the stopped container.

    **Note:** The `prepare-bulk` command by default saves files to a `bulk_import` directory. For the generated command to work, you must mount this directory into the container. You can do this by adding `- ./bulk_import:/bulk_import` to the `volumes` section of your `docker-compose.yml`.

3.  **Restart the Neo4j Service:**
    ```bash
    docker-compose up -d neo4j
    ```

### Step 4: Create Indexes

After a bulk import, the database will not have any constraints or indexes. You must create them manually using the `create-indexes` command.

```bash
omop2neo4j create-indexes
```

Your database is now ready to be used.

### Utility Commands

The following commands are also available:

*   **`clear-db`**: Use this command to only wipe the Neo4j database without loading new data.
    ```bash
    omop2neo4j clear-db
    ```
*   **`create-indexes`**: Use this to apply the schema (constraints and indexes) to an existing database. This is mainly useful after a manual or bulk import.
    ```bash
    omop2neo4j create-indexes
    ```
*   **`validate`**: Runs the post-load validation checks.
    ```bash
    omop2neo4j validate
    ```
