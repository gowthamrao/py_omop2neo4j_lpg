-- Create tables
CREATE TABLE concept (
    concept_id INTEGER NOT NULL,
    concept_name VARCHAR(255) NOT NULL,
    domain_id VARCHAR(20) NOT NULL,
    vocabulary_id VARCHAR(20) NOT NULL,
    concept_class_id VARCHAR(20) NOT NULL,
    standard_concept VARCHAR(1),
    concept_code VARCHAR(50) NOT NULL,
    valid_start_date DATE NOT NULL,
    valid_end_date DATE NOT NULL,
    invalid_reason VARCHAR(1)
);

CREATE TABLE vocabulary (
    vocabulary_id VARCHAR(20) NOT NULL,
    vocabulary_name VARCHAR(255) NOT NULL,
    vocabulary_reference VARCHAR(255),
    vocabulary_version VARCHAR(255),
    vocabulary_concept_id INTEGER NOT NULL
);

CREATE TABLE domain (
    domain_id VARCHAR(20) NOT NULL,
    domain_name VARCHAR(255) NOT NULL,
    domain_concept_id INTEGER NOT NULL
);

CREATE TABLE concept_class (
    concept_class_id VARCHAR(20) NOT NULL,
    concept_class_name VARCHAR(255) NOT NULL,
    concept_class_concept_id INTEGER NOT NULL
);

CREATE TABLE concept_relationship (
    concept_id_1 INTEGER NOT NULL,
    concept_id_2 INTEGER NOT NULL,
    relationship_id VARCHAR(20) NOT NULL,
    valid_start_date DATE NOT NULL,
    valid_end_date DATE NOT NULL,
    invalid_reason VARCHAR(1)
);

CREATE TABLE relationship (
    relationship_id VARCHAR(20) NOT NULL,
    relationship_name VARCHAR(255) NOT NULL,
    is_hierarchical VARCHAR(1) NOT NULL,
    defines_ancestry VARCHAR(1) NOT NULL,
    reverse_relationship_id VARCHAR(20) NOT NULL,
    relationship_concept_id INTEGER NOT NULL
);

CREATE TABLE concept_synonym (
    concept_id INTEGER NOT NULL,
    concept_synonym_name VARCHAR(1000) NOT NULL,
    language_concept_id INTEGER NOT NULL
);

CREATE TABLE concept_ancestor (
    ancestor_concept_id INTEGER NOT NULL,
    descendant_concept_id INTEGER NOT NULL,
    min_levels_of_separation INTEGER NOT NULL,
    max_levels_of_separation INTEGER NOT NULL
);

-- Insert sample data
INSERT INTO domain (domain_id, domain_name, domain_concept_id) VALUES
('Drug', 'Drug', 1),
('Condition', 'Condition', 2);

INSERT INTO vocabulary (vocabulary_id, vocabulary_name, vocabulary_reference, vocabulary_version, vocabulary_concept_id) VALUES
('RxNorm', 'RxNorm', 'ref1', 'v1', 101),
('SNOMED', 'SNOMED', 'ref2', 'v2', 102);

INSERT INTO concept (concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason) VALUES
(1001, 'Aspirin', 'Drug', 'RxNorm', 'Ingredient', 'S', 'A1', '2000-01-01', '2099-12-31', NULL),
(1002, 'Headache', 'Condition', 'SNOMED', 'Finding', 'S', 'B2', '2000-01-01', '2099-12-31', NULL),
(1003, 'Pain Killer', 'Drug', 'RxNorm', 'Ingredient', NULL, 'C3', '2000-01-01', '2099-12-31', NULL);

-- Insert sample data into relationship table
INSERT INTO relationship (relationship_id, relationship_name, is_hierarchical, defines_ancestry, reverse_relationship_id, relationship_concept_id) VALUES
('treats', 'treats', 'N', 'N', 'is treated by', 4289452),
('maps to', 'maps to', 'N', 'N', 'is mapped from', 44818752);

-- Note: We are not creating a separate concepts_optimized.csv, so we will use concept table and create synonyms from concept_synonym
INSERT INTO concept_synonym (concept_id, concept_synonym_name, language_concept_id) VALUES
(1001, 'acetylsalicylic acid', 4180186);

INSERT INTO concept_relationship (concept_id_1, concept_id_2, relationship_id, valid_start_date, valid_end_date, invalid_reason) VALUES
(1001, 1002, 'treats', '2000-01-01', '2099-12-31', NULL),
(1003, 1001, 'maps to', '2000-01-01', '2099-12-31', NULL);

INSERT INTO concept_ancestor (ancestor_concept_id, descendant_concept_id, min_levels_of_separation, max_levels_of_separation) VALUES
(1003, 1001, 1, 1);
