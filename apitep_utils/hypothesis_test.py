import logging
from enum import Enum

import pandas as pd
from scipy.stats import kruskal, levene, pearsonr, ranksums, spearmanr, shapiro

log = logging.getLogger(__name__)


class HypothesisTest:
    """
    HypothesisTest focused in determining if a target variable depends on a
    candidate variable. The specific test to perform must be selected by the
    user of the class.

    The test has two hypothesis:
    - H0 (null hypothesis): target variable depends on candidate variable.
    - H1 (alternative hypothesis): target variable does not depend on candidate
    variable.

    The properties of the class are:
    - test_type: test to perform, selected form a list of possible test
    available in the enumeration TestType.
    - dataframe: pandas data frame with the data needed to perform the test.
    - target: name of a pandas series with the values of the target variable.
    - candidate: a pandas series with the values of the candidate variable.
    - p_value: cut value for the H0 of the test to be true. False if the result
    of the test if greater than that.
    """

    class TestType(Enum):
        Pearson = "Pearson"
        Spearman = "Spearman"
        Levene = "Levene"
        KruskalWallis = "Kruskal-Wallis"
        WilcoxonRankSum = "Wilcoxon rank-sum"
        Shapiro = "Shapiro"

    test_type: TestType = TestType.Pearson
    dataframe: pd.DataFrame = None
    target: str = "target"
    candidate: str = "candidate"
    p_value: float = 0.05  # significance_level

    null_hypothesis_description: str = ""
    alternative_hypothesis_description: str = ""
    p: float = 0.0  # p_value
    stat: float = 0.0  # statistic_value

    def __init__(
            self,
            dataframe: pd.DataFrame = None,
            test_type: TestType = None,
            target: str = None,
            candidate: str = None,
            p_value: float = None
    ):
        """
        Create an instance of the tests.

        Always use named parameters to initialize this class. All the arguments
        are optional and fall back to default values if not provided.

        :param dataframe: pandas dataframe with the data the test should use.
        :param test_type: type of tests to perform.
        :param target: name of the pandas series with the values of the target
        variable, the one that is supposed to influence the candidate. It is one
        of the dataframe's columns.
        :param candidate: name of the pandas series with the values of the
        candidate variable, the one that is supposed to depend on the target
        variable. It is one of the dataframe's columns.
        :param p_value: two-tailed p-value.
        """

        log.info("Init tests")
        log.debug(f"Tests.__init__("
                  f"dataframe={len(dataframe.index)} rows, "
                  f"test_type={test_type}, "
                  f"target={target}, "
                  f"candidate={candidate}, "
                  f"p_value={p_value})")

        if dataframe is not None:
            self.dataframe = dataframe
        if test_type is not None:
            self.test_type = test_type
        if target is not None:
            self.target = target
        if candidate is not None:
            self.candidate = candidate
        if p_value is not None:
            self.p_value = p_value

    def execute(self) -> bool:
        """
        Perform the test select by the user through the property test_type.

        If the test selected is not implemented, the corresponding error will
        be raised.

        :return: True if H_0 is accepted, False otherwise.
        :rtype: bool
        """

        log.info("Execute test")
        log.debug("Tests.execute()")

        if self.test_type == HypothesisTest.TestType.Pearson:
            self.execute_pearson()
        elif self.test_type == HypothesisTest.TestType.Spearman:
            self.execute_spearman()
        elif self.test_type == HypothesisTest.TestType.Levene:
            self.execute_levene()
        elif self.test_type == HypothesisTest.TestType.KruskalWallis:
            self.execute_kruskal_wallis()
        elif self.test_type == HypothesisTest.TestType.WilcoxonRankSum:
            self.execute_wilcoxon_rank_sum()
        elif self.test_type == HypothesisTest.TestType.Shapiro:
            self.execute_shapiro()
        else:
            raise NotImplementedError

        if self.p <= self.p_value:
            result = True
        else:
            result = False

        self.log_results(test_result=result)

        return result

    def execute_pearson(self):
        """
        Perform a Pearson test.
        """

        log.info("Execute Pearson test")
        log.debug("Tests.execute_pearson()")

        self.null_hypothesis_description = "Pearson's Null Hypothesis Description"
        self.alternative_hypothesis_description = "Pearson's Alternative Hypothesis Description"

        x = self.dataframe[self.target]
        y = self.dataframe[self.candidate]
        self.stat, self.p = pearsonr(x, y)

    def execute_spearman(self):
        """
        Perform a Spearman test.
        """

        log.info("Execute Spearman test")
        log.debug("Tests.execute_spearman()")

        self.null_hypothesis_description = "Spearman's Null Hypothesis Description"
        self.alternative_hypothesis_description = "Spearman's Alternative Hypothesis Description"

        a = self.dataframe[self.target]
        b = self.dataframe[self.candidate]
        self.stat, self.p = spearmanr(a, b)

    def execute_levene(self):
        """
        Perform a Levene test.
        """

        log.info("Execute Levene test")
        log.debug("Tests.execute_levene()")

        self.null_hypothesis_description = "Levene's Null Hypothesis Description"
        self.alternative_hypothesis_description = "Levene's Alternative Hypothesis Description"

        a = self.dataframe[self.target]
        b = self.dataframe[self.candidate]
        self.stat, self.p = levene(a, b)

    def execute_shapiro(self):
        """
        Perform a Shapiro test.
        """

        log.info("Execute Shapiro test")
        log.debug("Tests.execute_shapiro()")

        self.null_hypothesis_description = "Shapiro Null Hypothesis Description"
        self.alternative_hypothesis_description = "Shapiro Alternative Hypothesis Description"

        a = self.dataframe[self.target]
        self.stat, self.p = shapiro(a)

    def execute_kruskal_wallis(self):
        """
        Perform a Kruskal-Wallis test.
        """

        log.info("Execute Kruskal Wallis test")
        log.debug("Tests.execute_kruskal_wallis()")

        self.null_hypothesis_description = "Kruskal-Wallis's Null Hypothesis Description"
        self.alternative_hypothesis_description = "Kruskal-Wallis's Alternative Hypothesis Description"

        a = self.dataframe[self.target]
        b = self.dataframe[self.candidate]
        self.stat, self.p = kruskal(a, b)

    def execute_wilcoxon_rank_sum(self):
        """
        Perform a Wilcoxon rank-sum test.
        """

        log.info("Execute Wilcoxon rank-sum test")
        log.debug("Tests.execute_wilcoxon_rank_sum()")

        self.null_hypothesis_description = "Wilcoxon rank-sum's Null Hypothesis Description"
        self.alternative_hypothesis_description = "Wilcoxon rank-sum's Alternative Hypothesis Description"

        a = self.dataframe[self.target]
        b = self.dataframe[self.candidate]
        self.stat, self.p = ranksums(a, b)

    def log_results(self, test_result: bool):
        """
        Log the test's results.

        :param test_result: result of the test.
        """

        log.info("Test's results")
        log.debug(f"Tests.log_results("
                  f"test_result={test_result})")

        log.info(f"- test name: {self.test_type.value}")
        log.info(f"- null hypothesis description: {self.null_hypothesis_description}")
        log.info(f"- alternative hypothesis description: {self.alternative_hypothesis_description}")
        if test_result:
            log.info(f"- alternative hypothesis accepted")
        else:
            log.info(f"- null hypothesis accepted")
        log.info(f"- cut value selected: {self.p_value}")  # fix description of the log entry
        log.info(f"- cut value obtained: {self.p}")  # fix description of the log entry
        log.info(f"- stat: {self.stat}")  # fix description of the log entry


