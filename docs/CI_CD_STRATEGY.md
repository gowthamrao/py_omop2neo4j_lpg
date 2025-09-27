# CI/CD Strategy and Architecture

## 1. Overview

This document outlines the architecture and rationale behind the modern, adaptive CI/CD pipeline implemented for the `py-omop2neo4j-lpg` repository. The primary goals of this pipeline are to ensure code quality, provide robust testing across multiple environments, enhance security, and automate the build and verification of containerized artifacts.

## 2. Technology Stack and Rationale

The CI/CD pipeline is built on a foundation of modern, best-practice tools tailored to the Python ecosystem:

-   **Dependency Management:** **Poetry** is used for managing project dependencies and virtual environments, providing reproducible builds.
-   **Code Quality:** **pre-commit**, **Ruff**, and **Mypy** form the core of our automated code quality framework.
    -   **Ruff** is used for high-speed linting and code formatting, ensuring a consistent style.
    -   **Mypy** provides static type checking to catch type-related errors before runtime.
-   **Testing:** **pytest** is used as the testing framework. Tests are logically separated using markers (`unit` and `integration`).
-   **Containerization:** **Docker** and **Docker Compose** are used for creating isolated, reproducible environments for integration testing and production deployments.
-   **CI/CD Platform:** **GitHub Actions** orchestrates all workflows, from testing to container scanning.

## 3. Proactive Improvements Made

Several key gaps were identified and addressed to modernize the repository and align it with best practices:

1.  **Initialized `pre-commit` for Standardized Linting:**
    -   **Gap:** The repository lacked any automated linting or formatting tools, leading to potential inconsistencies.
    -   **Improvement:** A `.pre-commit-config.yaml` file was created and configured with hooks for `Ruff` and `Mypy`. This enforces code quality standards automatically on every commit, both locally and in the CI pipeline.

2.  **Added Multi-Stage `Dockerfile` for Deployment:**
    -   **Gap:** The repository contained `docker-compose.yml` files for testing but was missing a `Dockerfile` for building a production-ready application image.
    -   **Improvement:** A new, multi-stage `Dockerfile` was created. This builds the application in a dedicated builder stage and copies only the necessary artifacts to a slim, non-root final image, resulting in a smaller and more secure container.

3.  **Consolidated Tool Configuration in `pyproject.toml`:**
    -   **Gap:** Configuration for development tools was absent.
    -   **Improvement:** The `pyproject.toml` file was updated to include `Ruff` and `Mypy` as development dependencies and to define their configurations, creating a single source of truth.

## 4. Workflow Architecture

The CI/CD system is composed of two distinct GitHub Actions workflows.

### a. `ci.yml` (Core CI Workflow)

This is the primary workflow responsible for continuous integration.

-   **Triggers:** Runs on `push` and `pull_request` events targeting the `main` branch.
-   **Job Structure:**
    -   **Matrix Testing:** A single job (`test`) runs across a matrix of operating systems (`ubuntu-latest`, `macos-latest`, `windows-latest`) and Python versions (`3.11`, `3.12`). This ensures the package is reliable across different environments.
    -   **Service Containers:** It spins up `postgres:13` and `neo4j:latest` containers to provide the necessary backing services for integration tests.
    -   **Caching:** The `actions/setup-python` action is configured to cache Poetry dependencies, significantly speeding up subsequent runs.
    -   **Test Execution:** It runs unit and integration tests in separate steps, using `pytest` markers (`-m unit` and `-m integration`) to distinguish between them.
-   **Key Steps:**
    1.  Check out the repository.
    2.  Set up the Python environment and Poetry.
    3.  Install dependencies with caching enabled.
    4.  Run `pre-commit` hooks to enforce code quality.
    5.  Run unit tests and generate `coverage-unit.xml`.
    6.  Run integration tests and generate `coverage-integration.xml`.
    7.  Upload both coverage reports to Codecov with unique flags.

### b. `docker.yml` (Docker Build and Scan Workflow)

This workflow focuses on building and securing the Docker image.

-   **Triggers:** Runs on `push` events to `main` or when the `Dockerfile` or source code in `src/` is modified.
-   **Job Structure:** A single job (`build-and-scan`) performs all Docker-related tasks.
-   **Key Steps:**
    1.  Check out the repository.
    2.  Set up Docker Buildx for efficient image building.
    3.  Build the Docker image using the GHA cache backend to accelerate the process. The image is loaded into the local runner but not pushed.
    4.  Scan the newly built image for vulnerabilities using **Trivy**. The workflow is configured to fail if any `CRITICAL` or `HIGH` severity vulnerabilities are found.

## 5. Testing Strategy

-   **Separation:** Tests are separated into `unit` and `integration` suites using `pytest` markers. This allows for faster feedback, as unit tests can run without the overhead of service containers.
-   **Code Coverage:**
    -   Coverage is measured separately for unit and integration tests.
    -   Reports (`coverage-unit.xml` and `coverage-integration.xml`) are uploaded to **Codecov** with distinct flags (e.g., `ubuntu-latest-py3.12-unit`). This provides a granular view of test coverage across different environments and test types.

## 6. Code Quality and Linting

-   Code quality is enforced by `pre-commit` hooks that run `Ruff` (linting and formatting) and `Mypy` (static type checking).
-   These checks are executed in the `ci.yml` workflow, ensuring that no pull request can be merged without adhering to the defined quality standards.

## 7. Dependency Management and Caching

-   **Poetry** manages all Python dependencies.
-   The `ci.yml` workflow uses the caching feature of `actions/setup-python` with `cache: 'poetry'`. The cache key is based on the hash of `poetry.lock`, ensuring that dependencies are re-installed only when the lock file changes.
-   The `docker.yml` workflow leverages the GHA cache backend with `docker/build-push-action`, which caches Docker layers to speed up subsequent image builds.

## 8. Security Hardening

The pipeline incorporates several security best practices:

-   **Principle of Least Privilege (PoLP):** Workflows are configured with `permissions: contents: read` by default to minimize their access rights.
-   **Action Pinning:** All third-party GitHub Actions are pinned to their full commit SHA. This mitigates supply chain risk by ensuring that only a specific, audited version of an action is executed.
-   **Container Security:**
    -   The `Dockerfile` uses a multi-stage build to create a minimal final image, reducing the attack surface.
    -   The final image runs under a non-root user (`appuser`).
    -   The `docker.yml` workflow includes a mandatory vulnerability scan with **Trivy**.

## 9. How to Run Locally

To replicate the CI checks locally, developers should install `pre-commit` and set up the environment.

1.  **Install `pre-commit`:**
    ```bash
    pip install pre-commit
    ```

2.  **Install the Git Hooks:**
    ```bash
    pre-commit install
    ```
    Now, the pre-commit hooks will run automatically on every `git commit`.

3.  **Run All Checks Manually:**
    To run all configured checks against all files, use:
    ```bash
    pre-commit run --all-files
    ```

4.  **Run Tests Locally:**
    First, ensure you have Poetry installed and have set up the project's virtual environment. Then, to run the tests as the CI does, you will need Docker and Docker Compose.

    -   **Start the test services:**
        ```bash
        docker-compose -f docker-compose.test.yml up -d
        ```
    -   **Run the tests:**
        ```bash
        poetry run pytest
        ```
    -   **Stop the test services:**
        ```bash
        docker-compose -f docker-compose.test.yml down
        ```
