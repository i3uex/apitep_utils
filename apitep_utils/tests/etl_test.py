import unittest

from apitep_utils import ETL


class DerivedETL(ETL):
    def process(self):
        self.output_df = self.input_df


class TestETL(unittest.TestCase):
    def test_etl_load(self):
        input_path_segment = "test_dataset.csv"
        output_path_segment = "test_dataset_processed.csv"

        with open(input_path_segment, "r") as file:
            file_lines = len(file.readlines()) - 1

        etl = DerivedETL(
            input_path_segment=input_path_segment,
            output_path_segment=output_path_segment)
        etl.load()

        dataset_rows = len(etl.input_df.index)

        self.assertEqual(
            dataset_rows,
            file_lines,
            f"Input dataset has {dataset_rows} but it should have {file_lines}")

    def test_etl_save(self):
        input_path_segment = "test_dataset.csv"
        output_path_segment = "test_dataset_processed.csv"

        etl = DerivedETL(
            input_path_segment=input_path_segment,
            output_path_segment=output_path_segment)
        etl.load()
        etl.process()
        etl.save()

        with open(output_path_segment, "r") as file:
            file_lines = len(file.readlines()) - 1
        dataset_rows = len(etl.output_df.index)

        self.assertEqual(
            dataset_rows,
            file_lines,
            f"Output dataset has {dataset_rows} but it should have {file_lines}")
