import argparse
import logging
from pathlib import Path

import pandas as pd

from apitep_utils import ArgumentParserHelper

log = logging.getLogger(__name__)


class ETL:
    """
    Common extract, transform, and load operations.

    Create an instance of this class, tell it where is the source dataset and
    where the resulting one should be saved, and process the changes needed.

    A report about source and destination datasets will be created.
    """

    description: str = "ETL"
    input_path_segment: str = None
    output_path_segment: str = None
    input_separator: str = ","
    output_separator: str = ","

    input_df: pd.DataFrame = None
    output_df: pd.DataFrame = None

    def __init__(
            self,
            input_path_segment: str = None,
            output_path_segment: str = None,
            input_separator: str = None,
            output_separator: str = None
    ):
        """
        Init ETL class instance.

        :param input_path_segment: path to the input CSV dataset to process.
        Optional.
        :param output_path_segment: path where the input CSV dataset, after
        being processed, should be stored. Optional.
        """

        log.info("Init ETL")
        log.debug(f"ETL.__init__("
                  f"input_path_segment={input_path_segment}, "
                  f"output_path_segment={output_path_segment})")

        if input_path_segment is not None:
            self.input_path_segment = input_path_segment

        if output_path_segment is not None:
            self.output_path_segment = output_path_segment

        if input_separator is not None:
            self.input_separator = input_separator

        if output_separator is not None:
            self.output_separator = output_separator

    def load(self):
        """
        Load the CSV dataset in the input path provided. Save a report in the
        same path, with the same name, but with HTML extension.
        """

        log.info("Load input dataset")
        log.debug("ETL.load()")

        if self.input_path_segment is None:
            log.debug("- input path is none, nothing to load or report about")
            return

        self.input_df = pd.read_csv(
            self.input_path_segment,
            sep=self.input_separator)
        self.save_report(self.input_df, self.input_path_segment)

    def save(self):
        """
        Save the CSV dataset in the output path provided. Save a report in the
        same path, with the same name, but with HTML extension.
        """

        log.info("Save output dataset")
        log.debug("ETL.save()")

        if self.output_path_segment is None:
            log.debug("- output path is none, nothing to save or report about")
            return

        output_path = Path(self.output_path_segment)
        output_path_parent = output_path.parent
        if not output_path_parent.exists():
            output_path_parent.mkdir(parents=True)

        self.output_df.to_csv(
            self.output_path_segment,
            sep=self.output_separator)
        self.save_report(self.output_df, self.output_path_segment)

    def process(self):
        """
        Make all the changes needed in the input dataframe to get the output
        dataframe.

        Make sure to use `input_df` as the input of your pipeline, and to store
        the resulting dataset in `output_df`.
        """

        log.info("Process dataset")
        log.debug("ETL.process()")

        self.output_df = self.input_df

        raise NotImplementedError

    def parse_arguments(self):
        """
        Parse arguments provided via command line, and check if they are valid
        or not. Adequate defaults are provided when possible.

        Parsed arguments are:
        - path to the input CSV dataset.
        - path to the output CSV dataset.
        """

        log.info("Get ETL arguments")
        log.debug("ETL.parse_arguments()")

        program_description = self.description
        argument_parser = argparse.ArgumentParser(description=program_description)
        argument_parser.add_argument("-i", "--input_path", required=True,
                                     help="path to the input CSV dataset")
        argument_parser.add_argument("-o", "--output_path", required=True,
                                     help="path to the output CSV dataset")

        arguments = argument_parser.parse_args()
        self.input_path_segment = ArgumentParserHelper.parse_data_file_path(
            data_file_path=arguments.input_path)
        self.output_path_segment = ArgumentParserHelper.parse_data_file_path(
            data_file_path=arguments.output_path,
            check_is_file=False)

    @staticmethod
    def save_report(df: pd.DataFrame, source_path_segment: str):
        """
        Save a report about the provided dataframe in the path provided,
        changing the extension to "html".

        :param df: dataframe a report should be generated about.
        :param source_path_segment: path to the CSV data source used to create
        the dataframe. It will be used to compose the output path.
        """

        log.info("Save dataset report")
        log.debug(f"ETL.save_report("
                  f"df={len(df.index)} rows,"
                  f"source_path_segment={source_path_segment})")

        from pandas_profiling import ProfileReport

        source_path = Path(source_path_segment)
        output_path = source_path.with_suffix(".html")
        log.debug(f"- output_path: {output_path}")

        profile = ProfileReport(df, title=source_path.stem)
        profile.to_file(str(output_path))
