import unittest
from pathlib import Path

from apitep_utils import DatasetSubsampler


class TestDatasetSubsampler(unittest.TestCase):

    def test_dataset_subsampler_init_fail(self):
        dataset_subsampler = DatasetSubsampler("non_existant_file.csv")
        self.assertEqual(
            dataset_subsampler.dataset_lines,
            0,
            "Lines in dataset should be 0 as the file does not exists.")

    def test_dataset_subsampler_init_success(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        self.assertGreater(
            dataset_subsampler.dataset_lines,
            0,
            "Lines in dataset should be greater than 0 as the file exists.")

    def test_dataset_subsampler_subsample_rows_less_than_zero(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_rows(-1)
        self.assertIsNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should be None")

    def test_dataset_subsampler_subsample_rows_zero(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_rows(0)
        self.assertIsNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should be None")

    def test_dataset_subsampler_subsample_rows_one(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_rows(1)
        self.assertIsNotNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should not be None")

    def test_dataset_subsampler_subsample_rows_greater_than(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_rows(1000)
        self.assertIsNotNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should not be None")

    def test_dataset_subsampler_subsample_percentage_less_than_zero(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_percentage(-1)
        self.assertIsNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should be None")

    def test_dataset_subsampler_subsample_percentage_zero(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_percentage(0)
        self.assertIsNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should be None")

    def test_dataset_subsampler_subsample_percentage_one(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_percentage(1)
        self.assertIsNotNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should not be None")

    def test_dataset_subsampler_subsample_percentage_greater_than(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv")
        dataset_subsampler.subsample_percentage(1000)
        self.assertIsNotNone(
            dataset_subsampler.dataset_subsample,
            "Dataset subsample should not be None")

    def test_dataset_subsampler_subsample_random(self):
        # I don't like that this test can fail if the randomly selected row
        # is the first.
        dataset_subsampler = DatasetSubsampler("test_dataset.csv", randomize=True)
        dataset_subsampler.subsample_rows(1)
        passenger_identifier_expected = 892
        passenger_identifier_obtained = dataset_subsampler.dataset_subsample.iloc[0]["PassengerId"]
        self.assertNotEqual(
            passenger_identifier_obtained,
            passenger_identifier_expected,
            f"Passenger identifier obtained ({passenger_identifier_obtained}) "
            f"should not be ({passenger_identifier_expected}).")
        pass

    def test_dataset_subsampler_subsample_not_random(self):
        dataset_subsampler = DatasetSubsampler("test_dataset.csv", randomize=False)
        dataset_subsampler.subsample_rows(1)
        passenger_identifier_expected = 892
        passenger_identifier_obtained = dataset_subsampler.dataset_subsample.iloc[0]["PassengerId"]
        self.assertNotEqual(
            passenger_identifier_obtained,
            passenger_identifier_expected,
            f"Passenger identifier obtained ({passenger_identifier_obtained}) "
            f"should be ({passenger_identifier_expected}).")
        pass

    def test_dataset_subsampler_subsample_dataset_subsample_path(self):
        dataset_subsample_path = "test_dataset_subsample.csv"
        dataset_subsampler = DatasetSubsampler("test_dataset.csv", dataset_subsample_path=dataset_subsample_path)
        dataset_subsampler.subsample_rows(1)
        dataset_subsample_path_exists = Path("test_dataset_subsample.csv").is_file()
        self.assertTrue(
            dataset_subsample_path_exists,
            f"File at \"{dataset_subsample_path_exists}\" should exist")

