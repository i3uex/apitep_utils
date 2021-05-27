import logging
import sys

import numpy as np

from apitep_utils.transformation import Transformation

log = logging.getLogger(__name__)


class ETL(Transformation):
    """
    Common extract, transform, and load operations.

    Create an instance of this class, tell it where is the source dataset and
    where the resulting one should be saved, and process the changes needed.

    A report about source and destination datasets will be created.
    """

    description: str = "ETL"

    def __init__(
            self,
            input_path_segment: str = None,
            output_path_segment: str = None,
            input_separator: str = None,
            output_separator: str = None,
            save_report_on_load: bool = None,
            save_report_on_save: bool = None,
            report_type: Transformation.ReportType = None,
            report_path_segment: str = None,
            input_type_excel: bool = None
    ):
        """
        Init ETL class instance.

        :param input_path_segment: path to the input CSV dataset to process.
        Optional.
        :param output_path_segment: path where the input CSV datasets, after
        being integrated, should be stored. Optional.
        :param input_separator: separator used in the input dataset. Optional.
        :param output_separator: separator used in the output dataset. Optional.
        :param save_report_on_load: save input dataset report if True. Optional.
        :param save_report_on_save: save output dataset report if True.
        Optional.
        :param report_type: control the type of the report saved if
        save_report_on_load or save_report_on_save are True. Optional.
        :param input_type_excel: load with configuration of excel if True. Optional.
        """

        log.info("Init ETL")
        log.debug(f"ETL.__init__("
                  f"input_path_segment={input_path_segment}, "
                  f"output_path_segment={output_path_segment}, "
                  f"input_separator={input_separator}, "
                  f"output_separator={output_separator}, "
                  f"save_report_on_load={save_report_on_load}, "
                  f"save_report_on_save={save_report_on_save}, "
                  f"report_type={report_type}, "
                  f"report_path_segment={report_path_segment}, "
                  f"input_type_excel={input_type_excel})")

        super().__init__(
            input_path_segment=input_path_segment,
            output_path_segment=output_path_segment,
            input_separator=input_separator,
            output_separator=output_separator,
            save_report_on_load=save_report_on_load,
            save_report_on_save=save_report_on_save,
            report_type=report_type,
            report_path_segment=report_path_segment,
            input_type_excel=input_type_excel
        )

        if save_report_on_load is None:
            self.save_report_on_load = True
        if save_report_on_save is None:
            self.save_report_on_save = True
        if report_type is None:
            self.report_type = Transformation.ReportType.Standard
        if report_path_segment is None:
            self.report_path_segment = input_path_segment
        if input_type_excel is None:
            self.input_type_excel = False

    def replace_column(self, source_column: str, destination_column: str) -> int:
        """
        Replace the destination column with the source column, then delete the
        source column. Before hand, count the differences in values between
        both columns and return that value.

        :param source_column:
        :param destination_column:

        :return: number of differences between both columns.
        :rtype: int
        """

        log.info("Replace one column with another")
        log.debug(f"ETL.replace_column("
                  f"source_column={source_column},"
                  f"destination_column={destination_column})")

        comparison = np.where(self.input_df[source_column] != self.input_df[destination_column])
        changes = len(comparison[0])

        self.input_df[destination_column] = self.input_df[source_column]
        self.input_df.drop(labels=[source_column], axis="columns", inplace=True)

        return changes

    def execute(self):
        """
        Perform all the task needed for the ETL to complete.
        """

        log.info("Execute ETL")
        log.debug("ETL.execute()")

        if len(sys.argv) > 1:
            self.parse_arguments()
        self.load()
        self.process()
        self.save()
        self.log_changes()
