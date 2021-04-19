import argparse
import logging
import time
from enum import Enum
from pathlib import Path

import pandas as pd
import swifter

from apitep_utils import ArgumentParserHelper

log = logging.getLogger(__name__)


class Transformation:
    """
    Common pandas dataset load, transform, and save operations.

    Create an instance of this class, tell it where is the source dataset and
    where the resulting one should be saved, and process the changes needed.

    Optionally, generate a reports about the source and destination datasets,
    on load or save.
    """

    class ReportType(Enum):
        Standard = "standard"
        Advanced = "advanced"
        Both = "both"

    description: str = "Transformation"
    changes = {}
    input_path_segment: str = None
    output_path_segment: str = None
    input_separator: str = ","
    output_separator: str = ","
    save_report_on_load: bool = True
    save_report_on_save: bool = True
    report_type: ReportType = ReportType.Advanced

    input_df: pd.DataFrame = None
    output_df: pd.DataFrame = None

    def __init__(
            self,
            input_path_segment: str = None,
            output_path_segment: str = None,
            input_separator: str = None,
            output_separator: str = None,
            save_report_on_load: bool = None,
            save_report_on_save: bool = None,
            report_type: ReportType = None
    ):
        """
        Init Transformation class instance.

        :param input_path_segment: path to the input CSV dataset to process.
        Optional.
        :param output_path_segment: path where the input CSV dataset, after
        being processed, should be stored. Optional.
        :param input_separator: separator used in the input dataset. Optional.
        :param output_separator: separator used in the output dataset. Optional.
        :param save_report_on_load: save input dataset report if True.
        :param save_report_on_save: save output dataset report if True.
        :param report_type: control the type of the report saved if
        save_report_on_load or save_report_on_save are True.
        """

        log.info("Init Transformation")
        log.debug(f"Transformation.__init__("
                  f"input_path_segment={input_path_segment}, "
                  f"output_path_segment={output_path_segment}, "
                  f"input_separator={input_separator}, "
                  f"output_separator={output_separator}, "
                  f"save_report_on_load={save_report_on_load}, "
                  f"save_report_on_save={save_report_on_save}, "
                  f"report_type={report_type})")

        if input_path_segment is not None:
            self.input_path_segment = input_path_segment

        if output_path_segment is not None:
            self.output_path_segment = output_path_segment

        if input_separator is not None:
            self.input_separator = input_separator

        if output_separator is not None:
            self.output_separator = output_separator

        if save_report_on_load is not None:
            self.save_report_on_load = save_report_on_load

        if save_report_on_save is not None:
            self.save_report_on_save = save_report_on_save

        if report_type is not None:
            self.report_type = report_type

    def load(self):
        """
        Load the CSV dataset in the input path provided. Optionally, save a
        report in the same path, with the same name, but with HTML extension.
        """

        log.info("Load input dataset")
        log.debug("Transformation.load()")

        if self.input_path_segment is None:
            log.debug("- input path is none, nothing to load or report about")
            return

        self.input_df = pd.read_csv(
            self.input_path_segment,
            sep=self.input_separator)

        if self.save_report_on_load:
            self.save_report(self.input_df, self.input_path_segment)

    def save(self):
        """
        Save the CSV dataset in the output path provided. Save a report in the
        same path, with the same name, but with the corresponding extension.
        """

        log.info("Save output dataset")
        log.debug("Transformation.save()")

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

        if self.save_report_on_save:
            self.save_report(self.output_df, self.output_path_segment)

    def process(self):
        """
        Make all the changes needed in the input dataframe to get the output
        dataframe.

        Make sure to use `input_df` as the input of your pipeline, and to store
        the resulting dataset in `output_df`.
        """

        log.info("Process dataset")
        log.debug("Transformation.process()")

        self.output_df = self.input_df

        raise NotImplementedError

    def log_changes(self):
        """
        Dump to log how many changes are made to the dataset.

        The changes should be stored in the property `changes` as pair key,
        value where the key is the description of the change, and the value is
        what actually happened.
        """

        log.info("Log dataset changes")
        log.debug("log_changes()")

        for key in self.changes:
            log.info(f"- {key}: {self.changes[key]}")

    def parse_arguments(self):
        """
        Parse arguments provided via command line, and check if they are valid
        or not. Adequate defaults are provided when possible.

        Parsed arguments are:
        - path to the input CSV dataset.
        - path to the output CSV dataset.
        """

        log.info("Get transformation arguments")
        log.debug("Transformation.parse_arguments()")

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

    def save_report(self, dataframe: pd.DataFrame, source_path_segment: str):
        """
        Save a report about the provided dataframe in the path provided,
        changing the extension as needed. The type of the report depends on the
        user preferences.
        """

        log.info("Save dataset report")
        log.debug(f"Transformation.save_report("
                  f"dataframe={len(dataframe.index)} rows, "
                  f"source_path_segment={source_path_segment})")

        if self.report_type == Transformation.ReportType.Standard:
            Transformation.save_standard_report(
                dataframe,
                source_path_segment)
        elif self.report_type == Transformation.ReportType.Advanced:
            Transformation.save_advanced_report(
                dataframe,
                source_path_segment)
        elif self.report_type == Transformation.ReportType.Both:
            Transformation.save_standard_report(
                dataframe,
                source_path_segment)
            Transformation.save_advanced_report(
                dataframe,
                source_path_segment)
        else:
            raise NotImplementedError

    @staticmethod
    def save_standard_report(dataframe: pd.DataFrame, source_path_segment: str):
        """
        Save a report about the provided dataframe in the path provided,
        using Pandas Profiling, changing the extension to "html".

        :param dataframe: dataframe a report should be generated about.
        :param source_path_segment: path to the CSV data source used to create
        the dataframe. It will be used to compose the output path.
        """

        log.info("Save dataset standard report")
        log.debug(f"Transformation.save_standard_report("
                  f"df={len(dataframe.index)} rows,"
                  f"source_path_segment={source_path_segment})")

        from pandas_profiling import ProfileReport

        source_path = Path(source_path_segment)
        output_path = source_path.with_suffix(".html")
        log.debug(f"- output_path: {output_path}")

        profile = ProfileReport(dataframe, title=source_path.stem)
        profile.to_file(str(output_path))

    @staticmethod
    def save_advanced_report(dataframe: pd.DataFrame, source_path_segment: str):
        """
        Save an advanced report about the provided dataframe in the path
        provided, changing the extension as needed.

        :param dataframe: dataframe a report should be generated about.
        :param source_path_segment: path to the CSV data source used to create
        the dataframe. It will be used to compose the output path.
        """

        log.info("Save dataset standard report")
        log.debug(f"Transformation.save_advanced_report("
                  f"df={len(dataframe.index)} rows,"
                  f"source_path_segment={source_path_segment})")

        pass

    @classmethod
    def stopwatch(cls, func):
        """
        Create the decorator @Transformation.stopwatch to easily measure the
        execution time of a method. Time is measured before and after the
        execution of the method. A debug log entry is added showing the
        difference.

        :param func: method to measure.
        """

        def wrapper(self):
            tic = time.perf_counter()
            func(self)
            toc = time.perf_counter()
            log.debug(f"- time: {toc - tic:0.4f} s")

        return wrapper

    @staticmethod
    def get_swifter_version() -> str:
        """
        Returns Swifter version. This is here just to avoid PyCharm removing
        Swifter's import.

        :return: Swifter version.
        :rtype: str
        """

        log.info("Get Swifter version")
        log.debug("Transformation.get_swifter_version()")

        return swifter.__version__
