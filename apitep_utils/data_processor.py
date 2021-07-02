import argparse
import logging
import time
from enum import Enum
from pathlib import Path

import pandas as pd
import swifter

from apitep_utils import ArgumentParserHelper
from apitep_utils.report import Report

log = logging.getLogger(__name__)


class DataProcessor:
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

    description: str = "DataProcessor"
    changes = {}
    input_path_segment: str = None
    output_path_segment: str = None
    input_separator: str = ","
    output_separator: str = ","
    save_report_on_load: bool = False
    save_report_on_save: bool = False
    report_type: ReportType = None
    report_path_segment: str = None
    input_type_excel: bool = False

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
            report_type: ReportType = None,
            report_path_segment: str = None,
            input_type_excel: bool = None
    ):
        """
        Init DataProcessor class instance.

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
        :param report_path_segment: where to save the reports. If not presents,
        they will be stored in the dataset's folder.
        :param input_type_excel: load with configuration of excel if True. Optional.
        """

        log.info("Init data processor")
        log.debug(f"DataProcessor.__init__("
                  f"input_path_segment={input_path_segment}, "
                  f"output_path_segment={output_path_segment}, "
                  f"input_separator={input_separator}, "
                  f"output_separator={output_separator}, "
                  f"save_report_on_load={save_report_on_load}, "
                  f"save_report_on_save={save_report_on_save}, "
                  f"report_type={report_type}, "
                  f"report_path_segment={report_path_segment}, "
                  f"input_type_excel={input_type_excel})")

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

        if report_path_segment is not None:
            self.report_path_segment = report_path_segment

        if input_type_excel is not None:
            self.input_type_excel = input_type_excel

    def load(self):
        """
        Load the CSV or Excel dataset in the input path provided. Optionally, save a
        report in the same path, with the same name, but with HTML extension.
        """

        log.info("Load input dataset")
        log.debug("DataProcessor.load()")

        if self.input_path_segment is None:
            log.debug("- input path is none, nothing to load or report about")
            return
        if not self.input_type_excel:
            self.input_df = pd.read_csv(
                self.input_path_segment,
                sep=self.input_separator)
        else:
            self.input_df = pd.concat([pd.read_excel(
                self.input_path_segment,
                header=0,
                sheet_name=None
            )])

        if self.save_report_on_load:
            self.save_report(self.input_df, self.input_path_segment)

    def save(self):
        """
        Save the CSV dataset in the output path provided. Save a report in the
        same path, with the same name, but with the corresponding extension.
        """

        log.info("Save output dataset")
        log.debug("DataProcessor.save()")

        if self.output_path_segment is None:
            log.debug("- output path is none, nothing to save or report about")
            return

        output_path = Path(self.output_path_segment)
        output_path_parent = output_path.parent
        if not output_path_parent.exists():
            output_path_parent.mkdir(parents=True)

        self.output_df.to_csv(
            self.output_path_segment,
            sep=self.output_separator,
            index=False)

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
        log.debug("DataProcessor.process()")

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
        log.debug("DataProcessor.log_changes()")

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

        log.info("Get data processor arguments")
        log.debug("DataProcessor.parse_arguments()")

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
        log.debug(f"DataProcessor.save_report("
                  f"dataframe={len(dataframe.index)} rows, "
                  f"source_path_segment={source_path_segment})")

        if self.report_type == DataProcessor.ReportType.Standard:
            self.save_standard_report(
                dataframe,
                source_path_segment)
        elif self.report_type == DataProcessor.ReportType.Advanced:
            self.save_advanced_report(
                dataframe,
                source_path_segment)
        elif self.report_type == DataProcessor.ReportType.Both:
            self.save_standard_report(
                dataframe,
                source_path_segment)
            self.save_advanced_report(
                dataframe,
                source_path_segment)
        else:
            raise NotImplementedError

    def save_standard_report(
            self,
            dataframe: pd.DataFrame,
            source_path_segment: str
    ):
        """
        Save a report about the provided dataframe in the path provided,
        using Pandas Profiling, changing the extension to "html".

        :param dataframe: dataframe a report should be generated about.
        :param source_path_segment: path to the CSV data source used to create
        the dataframe. It will be used to compose the output path.
        """

        log.info("Save dataset standard report")
        log.debug(f"DataProcessor.save_standard_report("
                  f"df={len(dataframe.index)} rows,"
                  f"source_path_segment={source_path_segment})")

        from pandas_profiling import ProfileReport

        source_path = Path(source_path_segment)
        name_segment = str(source_path.stem)

        if self.report_path_segment is not None:
            path_segment = self.report_path_segment
        else:
            path_segment = str(source_path.parent)

        output_path = Path(path_segment) / name_segment
        output_path = output_path.with_suffix(".html")
        log.debug(f"- output_path: {output_path}")

        profile = ProfileReport(dataframe, title=name_segment)
        profile.to_file(str(output_path))

    def save_advanced_report(
            self,
            dataframe: pd.DataFrame,
            source_path_segment: str
    ):
        """
        Save an advanced report about the provided dataframe in the path
        provided, changing the extension as needed.

        :param dataframe: dataframe a report should be generated about.
        :param source_path_segment: path to the CSV data source used to create
        the dataframe. It will be used to compose the output path.
        """

        log.info("Save dataset standard report")
        log.debug(f"DataProcessor.save_advanced_report("
                  f"df={len(dataframe.index)} rows,"
                  f"source_path_segment={source_path_segment})")

        source_path = Path(source_path_segment)
        name_segment = str(source_path.stem)

        if self.report_path_segment is not None:
            path_segment = self.report_path_segment
        else:
            path_segment = str(source_path.parent)

        report = Report()
        report.generate_advanced(
            ds=dataframe,
            name=name_segment,
            path=path_segment
        )

    @classmethod
    def stopwatch(cls, func):
        """
        Create the decorator @DataProcessor.stopwatch to easily measure the
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
        log.debug("DataProcessor.get_swifter_version()")

        return swifter.__version__
