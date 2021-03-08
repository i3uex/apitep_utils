from typing import List

import pandas as pd
import logging

from apitep_utils.dependency_test import DependencyTest

log = logging.getLogger(__name__)


class FeatureSelection:
    """
    Select the features that a given feature depends on after performing a
    series of dependency tests (specific hypothesis tests) on them.

    - dataframe: pandas dataframe with all the variables relevant to this study.
    - target_name: name of the column where the target variable values are.
    - dependency_tests: list of instances of the class DependencyTest containing
    the description of the tests to perform on each candidate to influence the
    target variable.
    - dependency_tests_results: list of results produced by the dependency
    tests.
    """

    dataframe: pd.DataFrame
    target_name: str
    dependency_tests: List[DependencyTest]
    dependency_tests_results: List[bool]

    def __init__(
            self,
            dataframe: pd.DataFrame,
            target_name: str,
            dependency_tests: List[DependencyTest]
    ):
        """
        Create an instance of the class. Just store the parameters provided in
        the corresponding class attributes.

        :param dataframe: pandas dataframe with the variables of the study.
        :param target_name: name of the column where the target variable values
        are.
        :param dependency_tests: list of instances of the class DependencyTest
        containing the description of the tests to perform on each candidate to
        influence the target variable.
        """

        log.info("Init feature selection")
        log.debug(f"FeatureSelection.__init__("
                  f"dataframe={dataframe}, "
                  f"target_name={target_name}, "
                  f"dependency_tests={dependency_tests})")

        self.dataframe = dataframe
        self.target_name = target_name
        self.dependency_tests = dependency_tests

    def process(self):
        """
        Perform each of the dependency tests provided. Save the results in the
        corresponding attribute.
        """

        log.info("Process feature selection")
        log.debug("FeatureSelection.process()")

        self.dependency_tests_results = []
        for dependency_test in self.dependency_tests:
            dependency_test_result = dependency_test.execute()
            self.dependency_tests_results.append(dependency_test_result)
