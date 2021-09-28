import argparse
import logging
import sys
from typing import List

import pandas as pd
from apitep_utils import ArgumentParserHelper
from apitep_utils.transformation import Transformation

log = logging.getLogger(__name__)


class Integration(Transformation):
    """
    Integrate multiple datasets into a single one.
    """

    description: str = "Integration"
    input_path_segments: List = None

    input_dfs: List = []

    def __init__(
            self,
            input_path_segments: List = None,
            output_path_segment: str = None,
            input_separator: str = None,
            output_separator: str = None,
            save_report_on_load: bool = None,
            save_report_on_save: bool = None,
            report_type: Transformation.ReportType = None,
    ):
        """
        Init Integration class instance.

        :param input_path_segments: list of paths to the input CSV datasets to
        integrate. Optional.
        :param output_path_segment: path where the input CSV datasets, after
        being integrated, should be stored. Optional.
        :param input_separator: separator used in the input dataset. Optional.
        :param output_separator: separator used in the output dataset. Optional.
        :param save_report_on_load: save input dataset report if True. Optional.
        :param save_report_on_save: save output dataset report if True.
        Optional.
        :param report_type: control the type of the report saved if
        save_report_on_load or save_report_on_save are True. Optional.
        """

        log.info("Init Integration")
        log.debug(f"Integration.__init__("
                  f"input_path_segments={input_path_segments}, "
                  f"output_path_segment={output_path_segment}, "
                  f"input_separator={input_separator}, "
                  f"output_separator={output_separator}, "
                  f"save_report_on_load={save_report_on_load}, "
                  f"save_report_on_save={save_report_on_save}, "
                  f"report_type={report_type})")

        super().__init__(
            input_path_segment=None,
            output_path_segment=output_path_segment,
            input_separator=input_separator,
            output_separator=output_separator,
            save_report_on_load=save_report_on_load,
            save_report_on_save=save_report_on_save,
            report_type=report_type
        )

        if input_path_segments is not None:
            self.input_path_segments = input_path_segments
        if save_report_on_load is None:
            self.save_report_on_load = False
        if save_report_on_save is None:
            self.save_report_on_save = True
        if report_type is None:
            self.report_type = Transformation.ReportType.Advanced

    def load(self):
        """
        Load the CSV datasets in the input path list provided. Optionally, save
        a report in the same path for each of them, with the same file name, but
        with HTML extension.
        """

        log.info("Load input datasets")
        log.debug("Integration.load()")

        if self.input_path_segments is None:
            log.debug("- input path is none, nothing to load or report about")
            return
        if not self.input_type_excel:
            for input_path_segment in self.input_path_segments:
                input_df = pd.read_csv(
                    input_path_segment,
                    sep=self.input_separator)
                self.input_dfs.append(input_df)
        else:
            for input_path_segment in self.input_path_segments:
                input_df = pd.concat(pd.read_excel(
                    input_path_segment,
                    header=0,
                    sheet_name=None
                ), ignore_index=True)
                self.input_dfs.append(input_df)

        if self.save_report_on_load:
            for index, input_df in enumerate(self.input_dfs):
                input_path_segment = self.input_path_segments[index]
                self.save_report(input_df, input_path_segment)

    def process(self):
        """
        Combine all the datasets in input_dfs into one.

        Provide your own version of this method if needed. Make sure to use
        `input_dfs` as the input of your pipeline, and to store the resulting
        dataset in `output_df`.
        """

        log.info("Process dataset")
        log.debug("Integration.process()")

        raise NotImplementedError

    def parse_arguments(self):
        """
        Parse arguments provided via command line, and check if they are valid
        or not. Adequate defaults are provided when possible.

        Parsed arguments are:
        - paths to the input CSV datasets, separated with spaces.
        - path to the output CSV dataset.
        """

        log.info("Get integration arguments")
        log.debug("Integration.parse_arguments()")

        program_description = self.description
        argument_parser = argparse.ArgumentParser(description=program_description)
        argument_parser.add_argument("-i", "--input_paths",
                                     required=True,
                                     nargs="+",
                                     help="path to the input CSV datasets")
        argument_parser.add_argument("-o", "--output_path", required=True,
                                     help="path to the output CSV dataset")

        arguments = argument_parser.parse_args()
        input_path_segments = arguments.input_paths
        self.input_path_segments = []
        for input_path_segment in input_path_segments:
            self.input_path_segments.append(
                ArgumentParserHelper.parse_data_file_path(
                    data_file_path=input_path_segment)
            )
        self.output_path_segment = ArgumentParserHelper.parse_data_file_path(
            data_file_path=arguments.output_path,
            check_is_file=False)

    def execute(self):
        """
        Perform all the task needed for the Integration to complete.
        """

        log.info("Execute Integration")
        log.debug("Integration.execute()")

        if len(sys.argv) > 1:
            self.parse_arguments()
        self.load()
        self.process()
        self.save()
        self.log_changes()
