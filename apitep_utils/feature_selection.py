from typing import List

import logging

from apitep_utils.hypothesis_test import HypothesisTest

log = logging.getLogger(__name__)


class FeatureSelection:
    """
    Select the features that a given feature depends on after performing a
    series of dependency tests (specific hypothesis tests) on them.

    - dependency_tests: list of instances of the class DependencyTest containing
    the description of the tests to perform on each candidate to influence the
    target variable.
    - influencing_features: list of results produced by the dependency
    tests.
    """

    dependency_tests: List[HypothesisTest]
    influencing_features: List = []
    not_influencing_features: List = []

    def __init__(
            self,
            dependency_tests: List[HypothesisTest],
    ):
        """
        Create an instance of the class. Just store the parameters provided in
        the corresponding class attributes.

        :param dependency_tests: list of instances of the class DependencyTest
        containing the description of the tests to perform on each candidate to
        influence the target variable.

        """

        log.info("Init feature selection")
        log.debug(f"FeatureSelection.__init__("
                  f"dependency_tests={dependency_tests})")

        self.dependency_tests = dependency_tests

    def process(self):
        """
        Perform each of the dependency tests provided. Save the results in the
        corresponding attribute.
        """

        log.info("Process feature selection")
        log.debug("FeatureSelection.process()")

        for dependency_test in self.dependency_tests:
            dependency_test_result = dependency_test.execute()
            if dependency_test_result:
                name = dependency_test.candidates[0].name
                self.influencing_features.append(name)
            else:
                name = dependency_test.candidates[0].name
                self.not_influencing_features.append(name)
        log.info("The list of influencing features are the next:")
        log.info(self.influencing_features)
        log.info("The list of not influencing features are the next:")
        log.info(self.not_influencing_features)

