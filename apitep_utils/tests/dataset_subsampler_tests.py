import unittest

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
