import logging
import random
from pathlib import Path

import pandas

from .timestamp import Timestamp

log = logging.getLogger(__name__)


class DatasetSubsampler:
    ROWS_KEY = "rows"
    PERCENTAGE_KEY = "percentage"
    FILE_NAME_SEPARATOR = "_"

    dataset_path: str = ""
    dataset_lines: int = 0
    dataset_subsample: pandas.DataFrame = None

    def __init__(self, dataset_path: str):
        """
        Init DatasetSubsampler class instance. Count the number of lines in the
        file and store that number in a private property. The first line of the
        dataset must be a header.

        :param dataset_path: path to the dataset to subsample.
        """

        log.info("Init DatasetSubsampler")
        log.debug(f"DatasetSubsampler.__init("
                  f"dataset_path={dataset_path}")

        self.dataset_path = dataset_path

        try:
            with open(self.dataset_path, "r") as file:
                self.dataset_lines = len(file.readlines())
        except IOError:
            log.error(f"- error opening file \"{self.dataset_path}\"")

    def subsample_rows(self, rows: int):
        """
        Create a subset of the dataset with the number of rows passed. If the
        number of rows of the subset is greater than the number of rows in the
        dataset, all the rows of the dataset will be returned.

        :param rows: number of rows the subsample must contain. If less or equal
        than zero, only the headers will be saved.
        """

        log.info("Subsample rows")
        log.debug(f"DatasetSubsampler.subsample_rows("
                  f"rows={rows}")

        rows_fixed = rows
        if rows_fixed <= 0:
            log.debug("- rows is less or equal to 0, subsample will not be saved")
            return
        elif rows_fixed > self.dataset_lines:
            log.debug("- "
                      "subsample rows is greater than dataset rows, a copy of the "
                      "dataset will be created")
            rows_fixed = self.dataset_lines

        if rows_fixed > 0:
            self.__load_dataset_rows(rows_fixed)
            self.__save_dataset_subsample(
                operation_key=DatasetSubsampler.ROWS_KEY,
                operation_value=rows
            )

    def subsample_percentage(self, percentage: int):
        """
        Create a subset of the dataset with the percentage or rows passed. If
        the percentage is greater than 100, all the rows of the dataset will be
        returned.

        :param percentage: percentage of the number of rows the subsample must
        contain. If less or equal than zero, only the headers will be saved.
        """

        log.info("Subsample percentage")
        log.debug(f"DatasetSubsampler.subsample_percentage("
                  f"percentage={percentage}")

        if percentage <= 0:
            log.debug("- percentage is less or equal to 0, subsample will not be saved")
            return
        elif percentage >= 100:
            rows = self.dataset_lines
        else:
            rows = int((self.dataset_lines - 1) * percentage / 100)

        if rows > 0:
            self.__load_dataset_rows(rows)
            self.__save_dataset_subsample(
                operation_key=DatasetSubsampler.PERCENTAGE_KEY,
                operation_value=percentage
            )

    def __load_dataset_rows(self, rows: int):
        """
        Read a number random rows from a CSV dataset. Never exclude row 0, that
        is, the header.

        :param rows: number of rows to read from the dataset.
        """

        log.info("Load rows from dataset")
        log.debug(f"DatasetSubsampler.__load_dataset_rows("
                  f"rows={rows}")

        skip_rows = sorted(random.sample(range(1, self.dataset_lines + 1), self.dataset_lines - rows))
        self.dataset_subsample = pandas.read_csv(
            self.dataset_path,
            dtype=str,
            skiprows=skip_rows)

    def __save_dataset_subsample(self, operation_key: str, operation_value: int):
        """
        Save the subsample in the same folder the original dataset is. Append
        three suffixes to the file name: a) "subset", b) subset operation
        indicator, and c) a timestamp.
        """

        log.info("Save data subsample")
        log.debug("DatasetSubsampler.__save_dataset_subsample(")

        path = Path(self.dataset_path)
        timestamp = Timestamp.file()
        output_filename = f"{path.stem}_subset_{operation_key}_{operation_value}_{timestamp}{path.suffix}"

        self.dataset_subsample.to_csv(output_filename, index=False)
