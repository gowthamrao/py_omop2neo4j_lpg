# Stage 1: Builder
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy dependency definition files
COPY pyproject.toml poetry.lock ./

# Install dependencies, including dev dependencies for building
RUN poetry install --no-root

# Copy the rest of the application source code
COPY . .

# Export production dependencies to requirements.txt
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes

# Stage 2: Final image
FROM python:3.11-slim as final

# Set working directory
WORKDIR /app

# Create a non-root user
RUN useradd --create-home appuser
USER appuser

# Copy requirements.txt from builder stage
COPY --from=builder /app/requirements.txt .

# Install production dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code from builder stage
COPY --from=builder /app/src ./src

# Set the entrypoint for the CLI application
ENTRYPOINT ["python", "-m", "src.py_omop2neo4j_lpg.cli"]
