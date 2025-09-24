# How-To Guide: Using omop2neo4j to Convert OMOP Vocabulary to Neo4j

This guide provides a comprehensive walkthrough of how to use the `omop2neo4j` package to convert OMOP vocabulary data from a PostgreSQL database into a Neo4j graph database.

## Overview

The `omop2neo4j` package is a Python-based tool designed to perform a robust and high-performance migration of OMOP vocabulary tables from a PostgreSQL database to a mature Labeled Property Graph (LPG) in Neo4j.

This package provides a suite of command-line tools to manage the Extract, Transform, Load (ETL) process. It is designed for performance and scalability, prioritizing the use of native database tools (`COPY` and `LOAD CSV`) and providing two main loading strategies:

1.  **Online Loading (`load-csv`):** An easy-to-use method that streams data directly into a running Neo4j instance using `LOAD CSV`. Ideal for most standard vocabulary sizes and when the Neo4j database is online.
2.  **Offline Loading (`prepare-bulk`):** A high-performance method for extremely large datasets. It prepares data files for Neo4j's offline `neo4j-admin database import` tool, which is significantly faster than `LOAD CSV` for bulk operations.

## Prerequisites

Before you begin, ensure you have the following installed and configured:

*   **Python:** Version 3.11 or higher.
*   **Docker and Docker Compose:** The recommended way to run the necessary Neo4j and PostgreSQL databases.
*   **OMOP Vocabulary CSV Files:** You need to download the OMOP Vocabulary files from the OHDSI website. The next section will provide instructions on how to do this.
*   **PostgreSQL:** A running instance of PostgreSQL. While you can use your own installation, this guide will include instructions on how to run it using Docker.

## Getting and Loading the OMOP Vocabulary

The first step is to download the OMOP Vocabulary from the OHDSI ATHENA website.

1.  **Download the Vocabulary:**
    *   Go to the [ATHENA website](http://athena.ohdsi.org/vocabulary/list).
    *   Log in or create an account if you don't have one.
    *   Select the vocabularies you need and add them to your cart.
    *   Click "Download" and you will receive an email with a link to download a zip file containing the vocabulary CSV files.

2.  **Prepare the Vocabulary Files:**
    *   Create a directory named `vocab` in the root of the `omop2neo4j` project.
    *   Unzip the downloaded file and place all the `.csv` files into the `vocab` directory.

3.  **Load the Vocabulary into PostgreSQL:**
    A helper script is provided in the `scripts` directory to load the OMOP vocabulary CSV files into your PostgreSQL database.

    *   **Run the script:** Before running the script, make sure you have created a `.env` file with your PostgreSQL connection details (see the "Setup" section below). Then, run the script from your terminal:

        ```bash
        python scripts/load_omop_vocab.py
        ```

## Setup

Now that you have the OMOP vocabulary loaded into PostgreSQL, you can set up the `omop2neo4j` package and the Neo4j database.

1.  **Clone the Repository:**
    If you haven't already, clone the `omop2neo4j` repository from GitHub:
    ```bash
    git clone <repository_url>
    cd omop2neo4j
    ```

2.  **Install the Package:**
    This project uses [Poetry](https://python-poetry.org/) for dependency management. It is recommended to install the package and its dependencies using Poetry.

    ```bash
    poetry install
    ```

    This will create a virtual environment and install all the necessary dependencies. To run commands, you can then use `poetry run <command>`.

3.  **Configure Environment Variables:**
    The application uses a `.env` file to manage database credentials and other settings.
    *   Copy the example file:
        ```bash
        cp .env.example .env
        ```
    *   Edit the `.env` file with your specific details. The following table lists all the available configuration options:

| Variable | Description | Default |
| --- | --- | --- |
| `POSTGRES_HOST` | The hostname or IP address of the PostgreSQL server. | `localhost` |
| `POSTGRES_PORT` | The port number of the PostgreSQL server. | `5432` |
| `POSTGRES_USER` | The username for the PostgreSQL database. | `postgres` |
| `POSTGRES_PASSWORD` | The password for the PostgreSQL database. | |
| `POSTGRES_DB` | The name of the PostgreSQL database. | `ohdsi` |
| `OMOP_SCHEMA` | The name of the OMOP CDM schema in the PostgreSQL database. | |
| `NEO4J_URI` | The URI for the Neo4j database. | `bolt://localhost:7687` |
| `NEO4J_USER` | The username for the Neo4j database. | `neo4j` |
| `NEO4J_PASSWORD` | The password for the Neo4j database. | |
| `EXPORT_DIR` | The directory where the CSV files will be exported. | `export` |
| `LOG_FILE` | The name of the log file. | `py-omop2neo4j-lpg.log` |
| `LOAD_CSV_BATCH_SIZE` | The batch size for `LOAD CSV` operations. | `10000` |
| `TRANSFORMATION_CHUNK_SIZE`| The number of rows to process per chunk for large files. | `100000` |

4.  **Start Neo4j with Docker:**
    The repository includes a `docker-compose.yml` file to easily start a Neo4j instance with the required configuration.
    ```bash
    docker-compose up -d
    ```
    This will start a Neo4j container in the background. You can view the logs with `docker-compose logs -f neo4j`.

## Online Workflow (load-csv)

This workflow is recommended for most standard vocabulary sizes. It streams data from CSV files directly into a running Neo4j instance.

### Step 1: Extract Data from PostgreSQL

Run the `extract` command to export the necessary vocabulary tables from your PostgreSQL database into CSV files. The files will be saved in the directory specified by `EXPORT_DIR` in your `.env` file (default: `./export`).

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

You can optionally specify the batch size for the `LOAD CSV` operations by using the `--batch-size` option. This can be useful for tuning performance on different systems.

```bash
omop2neo4j load-csv --batch-size 10000
```

The process can take several minutes depending on the size of the vocabulary and your hardware.

### Step 3: Validate the Loaded Data

After loading, run the `validate` command to check the integrity of the graph. This command runs a series of checks and prints a JSON report summarizing the results.

```bash
omop2neo4j validate
```

## Offline Workflow (prepare-bulk)

This workflow is recommended for very large vocabularies where the online `load-csv` method may be too slow or memory-intensive. This process involves generating formatted CSV files and then using the `neo4j-admin` tool while the database is offline.

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

You can customize the behavior of this command with the following options:

*   `--chunk-size`: The number of rows to process per chunk for large files. Default is `100000`.
*   `--import-dir`: The directory where the formatted files for the `neo4j-admin` import tool will be saved. Default is `bulk_import`.

Example:

```bash
omop2neo4j prepare-bulk --chunk-size 50000 --import-dir /tmp/bulk_import
```
At the end of the process, it will print the exact `neo4j-admin` command you need to run.

### Step 3: Run the Neo4j Admin Import

1.  **Stop the Neo4j Service:**
    ```bash
    docker-compose stop neo4j
    ```
2.  **Run the command:** Execute the exact command that was generated by the `prepare-bulk` step. The command will correctly use `docker-compose exec` to run the import inside the stopped container.

    **Note:** For the generated command to work, you must mount the `bulk_import` directory into the container. You can do this by adding `- ./bulk_import:/bulk_import` to the `volumes` section of your `docker-compose.yml`.

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

## Utility Commands

### Clearing the Neo4j Database

If you need to completely reset the Neo4j database, you can use the `clear-db` command. This will delete all nodes and relationships in the database, as well as drop all constraints and indexes.

```bash
omop2neo4j clear-db
```

**Warning:** This command is irreversible and will result in a clean, empty database.

## Connecting to Neo4j

Once the data is loaded, you can connect to the Neo4j database to explore and query the graph.

*   **Neo4j Browser:**
    *   Open your web browser and navigate to `http://localhost:7474`.
    *   You will be prompted for a username and password. Use the credentials you specified in your `.env` file (or the default `neo4j/StrongPass123` if you used the provided `docker-compose.yml`).
*   **Querying the Graph:**
    Here are a few example queries you can run in the Neo4j Browser to get started:

    *   **Count all nodes:**
        ```cypher
        MATCH (n) RETURN count(n);
        ```
    *   **Find a concept by name:**
        ```cypher
        MATCH (c:Concept) WHERE c.name = 'Pneumonia' RETURN c;
        ```
    *   **Find all ancestors of a concept:**
        ```cypher
        MATCH (c:Concept {name: 'Pneumonia'})-[:HAS_ANCESTOR]->(a) RETURN a;
        ```

## Troubleshooting

Here are some common issues you might encounter and how to resolve them.

*   **`docker-compose` command not found:**
    *   Make sure you have Docker and Docker Compose installed correctly. Refer to the official Docker documentation for installation instructions.

*   **Permission denied errors with Docker:**
    *   On Linux, you may need to run Docker commands with `sudo` or add your user to the `docker` group to run them without `sudo`.

*   **Connection errors to PostgreSQL or Neo4j:**
    *   Double-check your credentials in the `.env` file.
    *   Ensure that the database containers are running (`docker-compose ps`).
    *   Verify the host and port settings in your `.env` file. If you are running the databases on the same machine, `localhost` should be correct.

*   **`omop2neo4j` command not found:**
    *   Make sure you have installed the package using `poetry install`.
    *   If you are using a virtual environment, ensure it is activated by running `poetry shell`.

*   **`LOAD CSV` fails with file not found:**
    *   The `omop2neo4j` tool exports CSV files to the `./export` directory by default. The `docker-compose.yml` file mounts this directory to `/import` inside the Neo4j container. Ensure that this volume mount is correctly configured.
