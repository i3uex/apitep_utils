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
    randomize: bool = True
    dataset_lines: int = 0
    dataset_subsample: pandas.DataFrame = None
    dataset_subsample_path: str = ""

    def __init__(
            self,
            dataset_path: str,
            randomize: bool = True,
            dataset_subsample_path: str = None
    ):
        """
        Init DatasetSubsampler class instance. Count the number of lines in the
        file and store that number in a private property. The first line of the
        dataset must be a header.

        :param dataset_path: path to the dataset to subsample.
        :param randomize: if True, the subsample will be randomly selected; if
        False, the rows will be selected starting from the beginning.
        :param dataset_subsample_path: path where the dataset subsample should
        be stored.
        """

        log.info("Init DatasetSubsampler")
        log.debug(f"DatasetSubsampler.__init("
                  f"dataset_path={dataset_path}, "
                  f"randomize={randomize}, "
                  f"dataset_subsample_path={dataset_subsample_path}")

        self.dataset_path = dataset_path
        self.randomize = randomize
        self.dataset_subsample_path = dataset_subsample_path

        try:
            with open(self.dataset_path, "r") as file:
                self.dataset_lines = len(file.readlines()) - 1
        except IOError:
            log.error(f"- error opening file \"{self.dataset_path}\"")

    def subsample_rows(self, rows: int) -> str:
        """
        Create a subset of the dataset with the number of rows passed. If the
        number of rows of the subset is greater than the number of rows in the
        dataset, all the rows of the dataset will be returned.

        :param rows: number of rows the subsample must contain. If less or equal
        than zero, only the headers will be saved.

        :return: path to the file where the dataset subsample will be saved.
        :rtype: str
        """

        log.info("Subsample rows")
        log.debug(f"DatasetSubsampler.subsample_rows("
                  f"rows={rows}")

        dataset_subsample_path = ""

        rows_fixed = rows
        if rows_fixed <= 0:
            log.debug("- rows is less or equal to 0, subsample will not be saved")
            return dataset_subsample_path
        elif rows_fixed > self.dataset_lines:
            log.debug("- "
                      "subsample rows is greater than dataset rows, a copy of the "
                      "dataset will be created")
            rows_fixed = self.dataset_lines

        if rows_fixed > 0:
            self.__load_dataset_rows(rows_fixed)
            dataset_subsample_path = self.__save_dataset_subsample(
                operation_key=DatasetSubsampler.ROWS_KEY,
                operation_value=rows
            )

        return dataset_subsample_path

    def subsample_percentage(self, percentage: int):
        """
        Create a subset of the dataset with the percentage or rows passed. If
        the percentage is greater than 100, all the rows of the dataset will be
        returned.

        :param percentage: percentage of the number of rows the subsample must
        contain. If less or equal than zero, only the headers will be saved.

        :return: path to the file where the dataset subsample will be saved.
        :rtype: str
        """

        log.info("Subsample percentage")
        log.debug(f"DatasetSubsampler.subsample_percentage("
                  f"percentage={percentage}")

        dataset_subsample_path = ""

        if percentage <= 0:
            log.debug("- percentage is less or equal to 0, subsample will not be saved")
            return dataset_subsample_path
        elif percentage >= 100:
            rows = self.dataset_lines
        else:
            rows = int(self.dataset_lines * percentage / 100)

        if rows > 0:
            self.__load_dataset_rows(rows)
            dataset_subsample_path = self.__save_dataset_subsample(
                operation_key=DatasetSubsampler.PERCENTAGE_KEY,
                operation_value=percentage
            )

        return dataset_subsample_path

    def __load_dataset_rows(self, rows: int):
        """
        Read a number random rows from a CSV dataset. Never exclude row 0, that
        is, the header.

        :param rows: number of rows to read from the dataset.
        """

        log.info("Load rows from dataset")
        log.debug(f"DatasetSubsampler.__load_dataset_rows("
                  f"rows={rows}")

        if self.randomize:
            skip_rows = sorted(random.sample(range(1, self.dataset_lines + 1), self.dataset_lines - rows))
        else:
            skip_rows = range(1 + rows, self.dataset_lines + 1)

        self.dataset_subsample = pandas.read_csv(
            self.dataset_path,
            dtype=str,
            skiprows=skip_rows)

    def __save_dataset_subsample(self, operation_key: str, operation_value: int) -> str:
        """
        Save the subsample in the same folder the original dataset is. Append
        three suffixes to the file name: a) "subset", b) subset operation
        indicator, and c) a timestamp.

        :param operation_key: identifier of the subsample operation performed
        (rows or percentage).
        :param operation_value: value of the subsample operation performed
        (number of rows or percentage value).

        :return: path to the file where the dataset subsample will be saved.
        :rtype: str
        """

        log.info("Save data subsample")
        log.debug("DatasetSubsampler.__save_dataset_subsample(")

        path = Path(self.dataset_path)
        timestamp = Timestamp.file()
        if self.dataset_subsample_path == "":
            dataset_subsample_path = f"{path.stem}_subset_{operation_key}_{operation_value}_{timestamp}{path.suffix}"
        else:
            dataset_subsample_path = self.dataset_subsample_path

        self.dataset_subsample.to_csv(dataset_subsample_path, index=False)

        return dataset_subsample_path
