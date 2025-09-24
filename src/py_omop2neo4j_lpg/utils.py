import re


def standardize_label(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j Label (UpperCamelCase).
    - Splits the string by non-alphanumeric characters.
    - Capitalizes the first letter of each part, leaving other letters as-is.
    - Joins the parts.
    Example: "SpecAnatomicSite" -> "SpecAnatomicSite"
             "Drug/ingredient" -> "DrugIngredient"
             "mixedCASE" -> "MixedCASE"
    """
    if not s:
        return ""
    # Split by any character that is not a letter or number
    words = re.split(r"[^A-Za-z0-9]+", str(s))
    # Capitalize only the first letter of each word
    capitalized_words = [word[0].upper() + word[1:] if word else "" for word in words]
    return "".join(capitalized_words)


def standardize_reltype(s: str) -> str:
    """
    Sanitizes a string to be a valid Neo4j Relationship Type (UPPER_SNAKE_CASE).
    - Replaces groups of non-alphanumeric characters with a single underscore.
    - Converts to uppercase.
    - Removes any leading or trailing underscores.
    Example: "maps to" -> "MAPS_TO"
             "ATC - ATC" -> "ATC_ATC"
    """
    if not s:
        return ""
    # Replace any sequence of non-alphanumeric characters with a single space
    s = re.sub(r"[^A-Za-z0-9]+", " ", str(s)).strip()
    # Replace spaces with underscores and convert to uppercase
    return s.replace(" ", "_").upper()
