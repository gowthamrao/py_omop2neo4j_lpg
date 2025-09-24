# Business Requirements Document (BRD)

## 1. Introduction

This document outlines the business requirements for adding a new feature to the `py_omop2neo4j_lpg` tool. The feature will enable users to directly import OMOP (Observational Medical Outcomes Partnership) data from raw CSV files downloaded from OHDSI (Observational Health Data Sciences and Informatics) Athena.

## 2. Business Problem

Currently, the data import process for `py_omop2neo4j_lpg` may require manual preprocessing of data before it can be loaded. This adds extra steps for the user, increasing the time and complexity of using the tool. Users who download standardized vocabularies from OHDSI Athena receive them in a raw CSV format. A direct import mechanism for these files would streamline the user workflow significantly.

## 3. Goals and Objectives

The primary goal of this feature is to simplify the data import process and improve the user experience.

*   **Goal 1:** Allow direct import of raw CSV files from OHDSI Athena.
    *   **Objective 1.1:** Implement a mechanism to handle the specific format and structure of Athena CSV files.
    *   **Objective 1.2:** Eliminate the need for users to pre-process or reformat the CSV files before import.
*   **Goal 2:** Improve the robustness of the bulk import process.
    *   **Objective 2.1:** Resolve issues with the `neo4j-admin` bulk import, specifically concerning ID groups, to ensure reliable data loading.

## 4. Scope

### In-Scope

*   Adding a new feature or command-line option to accept a directory of CSV files.
*   Parsing and processing raw CSV files from OHDSI Athena.
*   Transforming the data from the CSVs into a format suitable for Neo4j.
*   Fixing the `neo4j-admin` bulk import process for ID groups.

### Out-of-Scope

*   Support for CSV formats other than those provided by OHDSI Athena.
*   A graphical user interface (GUI) for file uploads. The feature will be implemented as part of the command-line interface (CLI).
*   Data validation beyond ensuring the basic structure required for import.

## 5. Success Metrics

*   **Metric 1:** Users can successfully import a standard set of OHDSI Athena vocabulary CSVs into Neo4j using the new feature without any manual file modifications.
*   **Metric 2:** The time taken to import data is reasonable for typical vocabulary sizes.
*   **Metric 3:** The `neo4j-admin` bulk import completes without errors related to ID groups.
