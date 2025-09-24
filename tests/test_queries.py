import pytest
from py_omop2neo4j_lpg import extraction, loading

# --- Tests for extraction.py ---


def test_get_sql_queries():
    """
    Tests the SQL query generation for extraction.
    """
    schema = "test_schema"
    queries = extraction.get_sql_queries(schema)

    # Test that all expected keys are present
    expected_keys = [
        "concepts_optimized.csv",
        "domain.csv",
        "vocabulary.csv",
        "concept_relationship.csv",
        "concept_ancestor.csv",
    ]
    assert all(key in queries for key in expected_keys)

    # Test the concepts_optimized query for correct formatting
    concepts_query = queries["concepts_optimized.csv"]
    assert f"FROM\n                    {schema}.concept c" in concepts_query
    assert "string_agg(cs.concept_synonym_name, '|')" in concepts_query
    assert "TO STDOUT WITH CSV HEADER FORCE QUOTE *" in concepts_query

    # Test a simple query
    domain_query = queries["domain.csv"]
    assert f"COPY (SELECT * FROM {schema}.domain) TO STDOUT" in domain_query


# --- Tests for loading.py ---


@pytest.mark.parametrize("batch_size", [5000, 20000])
def test_get_loading_queries(batch_size):
    """
    Tests the Cypher query generation for loading.
    """
    queries = loading.get_loading_queries(batch_size)

    # Expecting 5 queries: domains, vocabularies, concepts, relationships, ancestors
    assert len(queries) == 5

    # --- Test Concept Loading Query ---
    concept_query = queries[2]
    assert (
        "LOAD CSV WITH HEADERS FROM 'file:///concepts_optimized.csv' AS row"
        in concept_query
    )
    # Check for the robust label standardization logic
    assert (
        "apoc.text.upperCamelCase(apoc.text.regreplace(row.domain_id, '[^A-Za-z0-9]+', ' '))"
        in concept_query
    )
    # Check for conditional standard label
    assert (
            "FOREACH (x IN CASE WHEN row.standard_concept = 'S' THEN [1] ELSE [] END |"
            in concept_query
    )
    # Check for batching
    assert f"IN TRANSACTIONS OF {batch_size} ROWS" in concept_query
    # Check for date conversion and synonym splitting
    assert "valid_start_date: date(row.valid_start_date)" in concept_query
    assert "split(row.synonyms, '|')" in concept_query

    # --- Test Relationship Loading Query ---
    relationship_query = queries[3]
    assert (
        "LOAD CSV WITH HEADERS FROM 'file:///concept_relationship.csv' AS row"
        in relationship_query
    )
    # Check for robust reltype standardization
    assert (
        "toupper(apoc.text.replace(row.relationship_id, '[^A-Za-z0-9_]+', '_'))"
        in relationship_query
    )
    # Check for batching
    assert f"IN TRANSACTIONS OF {batch_size} ROWS" in relationship_query

    # --- Test Ancestor Loading Query ---
    ancestor_query = queries[4]
    assert (
        "LOAD CSV WITH HEADERS FROM 'file:///concept_ancestor.csv' AS row"
        in ancestor_query
    )
    assert "CREATE (d)-[r:HAS_ANCESTOR]->(a)" in ancestor_query
    assert (
        "SET r.min_levels = toInteger(row.min_levels_of_separation)" in ancestor_query
    )
    # Check for batching
    assert f"IN TRANSACTIONS OF {batch_size} ROWS" in ancestor_query


def test_get_loading_queries_uses_correct_batch_size():
    """
    Ensures a different batch size is correctly inserted into the queries.
    """
    custom_batch_size = 9999
    queries = loading.get_loading_queries(custom_batch_size)
    concept_query = queries[2]
    relationship_query = queries[3]
    ancestor_query = queries[4]

    assert f"IN TRANSACTIONS OF {custom_batch_size} ROWS" in concept_query
    assert f"IN TRANSACTIONS OF {custom_batch_size} ROWS" in relationship_query
    assert f"IN TRANSACTIONS OF {custom_batch_size} ROWS" in ancestor_query
