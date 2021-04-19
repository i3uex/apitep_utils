import unittest

import pandas as pd
from apitep_utils.integration import Integration


class IntegrationExample(Integration):
    def process(self):
        result_df = None
        for input_df in self.input_dfs:
            if result_df is None:
                result_df = input_df.copy()
            else:
                result_df = pd.concat([result_df, input_df], axis=1)
        self.output_df = result_df


class TestIntegration(unittest.TestCase):
    def test_integration(self):
        integration = IntegrationExample(
            input_path_segments=[
                "test_dataset.csv",
                "test_dataset_column.csv"],
            output_path_segment="test_dataset_integration.csv")
        integration.execute()
