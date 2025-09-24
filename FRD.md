# Functional Requirements Document (FRD)

## 1. Introduction

This document provides a detailed description of the functional requirements for the CSV upload feature in the `py_omop2neo4j_lpg` tool. It is based on the changes proposed in pull request #94.

## 2. Functional Requirements

### 2.1. Command-Line Interface (CLI)

*   **FR-1:** The system shall provide a new command-line option to initiate the import process from a directory of CSV files.
*   **FR-2:** The CLI shall accept a path to a directory containing the raw CSV files from OHDSI Athena.

### 2.2. CSV Data Processing

*   **FR-3:** The system shall read all `.csv` files from the specified input directory.
*   **FR-4:** The system shall parse the CSV files, assuming they are tab-delimited as is standard for OHDSI Athena downloads.
*   **FR-5:** The system shall handle the specific column headers and data types present in the Athena CSV files.

### 2.3. Data Transformation and Loading

*   **FR-6:** The system shall transform the data from the CSVs into a format suitable for Neo4j import.
*   **FR-7:** The system shall correctly handle multi-label concepts. Specifically, it must parse and transform labels in formats like `'Concept;Drug;Standard'` into a colon-separated format `'Concept:Drug:Standard'` for use as Neo4j labels.
*   **FR-8:** The system shall add a `Vocabulary` label to all vocabulary nodes created during the import.
*   **FR-9:** The system shall use the `neo4j-admin` bulk import tool for efficient data loading.
*   **FR-10:** The system shall correctly configure ID groups for the `neo4j-admin` import to prevent data integrity issues.

### 2.4. Non-Functional Requirements

*   **NFR-1 (Performance):** The import process should be optimized for performance, handling large vocabulary datasets efficiently. The changes include memory constraints for environments with limited resources.
*   **NFR-2 (Error Handling):** The system should provide clear error messages if the import process fails (e.g., if the input directory is not found, or if CSV files are malformed).

## 3. Data Flow

1.  The user executes the `py_omop2neo4j_lpg` tool with the appropriate command-line option, providing the path to the directory containing the OHDSI Athena CSV files.
2.  The tool scans the directory for `.csv` files.
3.  For each CSV file, the tool reads the data, parsing it as a tab-delimited file.
4.  The data is transformed:
    *   Concept labels are re-formatted.
    *   Vocabulary nodes are assigned the `Vocabulary` label.
    *   Data is prepared for the `neo4j-admin` import, with correct ID groups.
5.  The tool invokes the `neo4j-admin` bulk import command to load the transformed data into the Neo4j database.
6.  Upon completion, the tool reports success or failure to the user.
