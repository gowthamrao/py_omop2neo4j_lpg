from __future__ import annotations
from neo4j import Driver
from .config import get_logger
from .loading import get_driver
import json

logger = get_logger(__name__)


def get_node_counts(driver: Driver) -> dict[str, int]:
    """
    Counts nodes for each distinct combination of labels in the database.
    This provides a detailed breakdown, e.g., for 'Concept:Drug:Standard'.
    """
    logger.info("Performing node count validation by label combination...")
    query = """
    MATCH (n)
    WITH labels(n) AS label_combination
    RETURN label_combination, count(*) AS count
    ORDER BY count DESC
    """
    with driver.session() as session:
        result = session.run(query)
        # Sort labels within each combination for consistent keys, e.g.,
        # ['Standard', 'Concept', 'Drug'] becomes 'Concept:Drug:Standard'
        counts = {
            ":".join(sorted(record["label_combination"])): record["count"]
            for record in result
            if record["label_combination"]
        }
        logger.info(f"Node counts by label combination: {json.dumps(counts, indent=2)}")
        return counts


def get_relationship_counts(driver: Driver) -> dict[str, int]:
    """
    Counts relationships for each distinct type in the database.
    """
    logger.info("Performing relationship count validation by type...")
    query = """
    CALL db.relationshipTypes() YIELD relationshipType
    CALL apoc.cypher.run('MATCH ()-[:`' + relationshipType + '`]->() RETURN count(*) as count', {}) YIELD value
    RETURN relationshipType, value.count AS count
    ORDER BY relationshipType
    """
    with driver.session() as session:
        result = session.run(query)
        counts = {record["relationshipType"]: record["count"] for record in result}
        logger.info(f"Relationship counts: {json.dumps(counts, indent=2)}")
        return counts


def verify_sample_concept(driver: Driver, concept_id: int = 1177480) -> dict | None:
    """
    Fetches a sample concept, its direct neighborhood, and its ancestors to verify its structure.
    The default concept_id is 1177480 ('Enalapril').
    """
    logger.info(f"Performing structural validation for Concept ID: {concept_id}...")
    # This query collects outgoing relationships by type, and incoming ancestors.
    query = """
    MATCH (c:Concept {concept_id: $concept_id})
    // Collect outgoing relationships and connected neighbors
    CALL {
        WITH c
        MATCH (c)-[r]->(neighbor)
        RETURN type(r) AS rel_type,
               collect({name: neighbor.name, id: COALESCE(neighbor.concept_id, neighbor.domain_id, neighbor.vocabulary_id)}) AS neighbors
    }
    // Collect incoming ancestors separately
    WITH c, collect({rel_type: rel_type, neighbors: neighbors}) as relationships
    // Filter out empty relationship placeholders that can occur if a node has no relationships
    WITH c, [rel IN relationships WHERE rel.rel_type IS NOT NULL] as relationships
    OPTIONAL MATCH (ancestor:Concept)-[:HAS_ANCESTOR]->(c)
    WITH c, relationships, collect(
        CASE WHEN ancestor IS NOT NULL THEN {name: ancestor.name, id: ancestor.concept_id} ELSE null END
    ) as ancestors
    RETURN
        c.concept_id AS concept_id,
        c.name AS name,
        labels(c) AS labels,
        size(c.synonyms) AS synonym_count,
        relationships,
        // Filter out the [null] that can be returned by the collect if no ancestors are found
        [ancestor IN ancestors WHERE ancestor IS NOT NULL] as ancestors
    """
    with driver.session() as session:
        result = session.run(query, concept_id=concept_id).single()
        if not result or not result.get("concept_id"):
            logger.warning(f"Sample Concept ID {concept_id} not found in the database.")
            return None

        record_dict = result.data()

        # Clean up the relationships aggregation for better logging
        rels_summary = {}
        for item in record_dict.pop("relationships", []):
            if item["rel_type"]:
                rels_summary[item["rel_type"]] = {
                    "count": len(item["neighbors"]),
                    "sample_neighbors": [
                        n["name"] for n in item["neighbors"][:3]
                    ],  # Show first 3
                }
        record_dict["relationships_summary"] = rels_summary

        # Add ancestors summary
        ancestors_list = record_dict.pop("ancestors", [])
        record_dict["ancestors_summary"] = {
            "count": len(ancestors_list),
            "sample_ancestors": [a["name"] for a in ancestors_list[:5]],  # Show first 5
        }

        # Sort labels for consistent output
        if "labels" in record_dict and record_dict["labels"]:
            record_dict["labels"] = sorted(record_dict["labels"])

        logger.info(
            f"Structural validation for '{record_dict.get('name')}': \n{json.dumps(record_dict, indent=2)}"
        )
        return record_dict


def run_validation():
    """
    Main orchestrator for the validation process.
    Connects to Neo4j and runs all validation checks.
    """
    logger.info("Starting validation process...")
    driver = None
    try:
        driver = get_driver()
        node_counts = get_node_counts(driver)
        rel_counts = get_relationship_counts(driver)
        sample_verification = verify_sample_concept(driver)  # Using default ID

        # A successful run returns a dictionary of the results
        return {
            "node_counts_by_label_combination": node_counts,
            "relationship_counts_by_type": rel_counts,
            "sample_concept_verification": sample_verification,
        }

    except Exception as e:
        logger.error(f"An error occurred during the validation process: {e}")
        # We return a dict so the CLI can handle the error gracefully
        return {"error": str(e)}
    finally:
        if driver:
            driver.close()
            logger.info("Validation process finished. Neo4j connection closed.")
