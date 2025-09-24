# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

def load_vocabulary_to_postgres():
    """
    Connects to a PostgreSQL database and loads OMOP vocabulary CSV files
    from a specified directory into the appropriate tables.
    """
    load_dotenv()

    # Database connection details from environment variables
    server = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    username = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    database = os.getenv("POSTGRES_DB")
    cdm_schema = os.getenv("OMOP_SCHEMA")
    vocab_file_dir = "vocab"

    conn = None
    try:
        # Establish connection
        conn = psycopg2.connect(
            dbname=database,
            user=username,
            password=password,
            host=server,
            port=port
        )
        cursor = conn.cursor()
        print("Successfully connected to PostgreSQL.")

        # List of vocabulary tables to load
        vocab_tables = [
            "CONCEPT", "VOCABULARY", "DOMAIN", "CONCEPT_CLASS",
            "CONCEPT_RELATIONSHIP", "RELATIONSHIP", "CONCEPT_SYNONYM",
            "CONCEPT_ANCESTOR", "DRUG_STRENGTH"
        ]

        for table_name in vocab_tables:
            file_path = os.path.join(vocab_file_dir, f"{table_name}.csv")
            if os.path.exists(file_path):
                print(f"Loading data from {file_path} into {cdm_schema}.{table_name}...")
                # Use pandas to read CSV in chunks
                for chunk in pd.read_csv(file_path, sep='\\t', chunksize=10000, quoting=3, on_bad_lines='warn'):
                    # Create a temporary in-memory file for the chunk
                    from io import StringIO
                    buffer = StringIO()
                    chunk.to_csv(buffer, index=False, header=False, sep='\\t')
                    buffer.seek(0)

                    # Use COPY FROM for efficient loading
                    cursor.copy_expert(f"COPY {cdm_schema}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER E'\\t'", buffer)
                conn.commit()
                print(f"Successfully loaded {table_name}.")
            else:
                print(f"Warning: {file_path} not found. Skipping.")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to or working with PostgreSQL", error)
    finally:
        # Closing database connection.
        if conn:
            cursor.close()
            conn.close()
            print("PostgreSQL connection is closed.")

if __name__ == "__main__":
    load_vocabulary_to_postgres()
