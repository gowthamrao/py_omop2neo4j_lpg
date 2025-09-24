# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.

import unittest
from py_omop2neo4j_lpg.utils import standardize_label, standardize_reltype


class TestUtils(unittest.TestCase):

    def test_standardize_label(self):
        test_cases = [
            ("Hello World", "HelloWorld"),
            ("Drug/Ingredient", "DrugIngredient"),
            ("SpecAnatomicSite", "SpecAnatomicSite"),
            ("Observation 2", "Observation2"),
            ("  leading spaces", "LeadingSpaces"),
            ("trailing spaces  ", "TrailingSpaces"),
            ("", ""),
            (None, ""),
            ("special-chars!@#$", "SpecialChars"),
            ("a b c", "ABC"),
            ("mixedCASE", "MixedCASE"),
        ]
        for input_str, expected_output in test_cases:
            with self.subTest(input=input_str):
                self.assertEqual(standardize_label(input_str), expected_output)

    def test_standardize_reltype(self):
        test_cases = [
            ("maps to", "MAPS_TO"),
            ("ATC - ATC", "ATC_ATC"),
            ("Has ancestor", "HAS_ANCESTOR"),
            ("RxNorm has ingredient", "RXNORM_HAS_INGREDIENT"),
            # Note: The original test for these was a bit ambiguous.
            # The function's behavior is to replace non-alphanum with space,
            # strip, then replace space with underscore.
            ("trailing_sep_", "TRAILING_SEP"),
            ("_leading_sep", "LEADING_SEP"),
            ("double__underscore", "DOUBLE_UNDERSCORE"),
            ("", ""),
            (None, ""),
            ("special-chars!@#$", "SPECIAL_CHARS"),
            ("a b c", "A_B_C"),
        ]
        for input_str, expected_output in test_cases:
            with self.subTest(input=input_str):
                self.assertEqual(standardize_reltype(input_str), expected_output)


if __name__ == "__main__":
    unittest.main()
