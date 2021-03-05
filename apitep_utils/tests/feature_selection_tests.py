import unittest

from apitep_utils import DependencyTest, FeatureSelection


class TestFeatureSelection(unittest.TestCase):
    def test_feature_selection(self):
        # Look for a nice-looking way of declaring a list of tests to perform
        # and then pass to a feature selection process.

        self.assertEqual(True, False)
