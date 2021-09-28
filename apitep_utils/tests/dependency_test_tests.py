import unittest

import pandas as pd

from apitep_utils import DependencyTest, FeatureSelection


class TestDependencyTest(unittest.TestCase):
    def test_dependency_test(self):

        df = pd.read_csv("test_dataset.csv")

        dependency_test_1 = DependencyTest(
            dataframe=df,
            test_type=DependencyTest.TestType.Pearson,
            target="Sex",
            candidate="Age",
            p_value=0.05
        )
        dependency_test_2 = DependencyTest(
            dataframe=df,
            test_type=DependencyTest.TestType.Pearson,
            target="Sex",
            candidate="Age",
            p_value=0.05
        )
        dependency_test_3 = DependencyTest(
            dataframe=df,
            test_type=DependencyTest.TestType.Pearson,
            target="Sex",
            candidate="Age",
            p_value=0.05
        )
        dependency_tests = [
            dependency_test_1,
            dependency_test_2,
            dependency_test_3
        ]

        feature_selection = FeatureSelection(
            dataframe=df,
            target_name="Sex",
            dependency_tests=dependency_tests
        )
