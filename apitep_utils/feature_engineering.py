import logging
import sys

from apitep_utils.transformation import Transformation

log = logging.getLogger(__name__)


class FeatureEngineering(Transformation):
    """
    Extract features from raw data using domain knowledge from previous steps.
    """

    description: str = "Feature Engineering"

    def __init__(
            self,
            input_path_segment: str = None,
            output_path_segment: str = None,
            input_separator: str = None,
            output_separator: str = None,
            save_report_on_load: bool = None,
            save_report_on_save: bool = None,
            report_type: Transformation.ReportType = None
    ):
        """
        Init Integration class instance.

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
        """

        log.info("Init FeatureEngineering")
        log.debug(f"FeatureEngineering.__init__("
                  f"input_path_segment={input_path_segment}, "
                  f"output_path_segment={output_path_segment}, "
                  f"input_separator={input_separator}, "
                  f"output_separator={output_separator}, "
                  f"save_report_on_load={save_report_on_load}, "
                  f"save_report_on_save={save_report_on_save}, "
                  f"report_type={report_type})")

        super().__init__(
            input_path_segment=input_path_segment,
            output_path_segment=output_path_segment,
            input_separator=input_separator,
            output_separator=output_separator,
            save_report_on_load=save_report_on_load,
            save_report_on_save=save_report_on_save,
            report_type=report_type
        )

        if save_report_on_load is None:
            self.save_report_on_load = False
        if save_report_on_save is None:
            self.save_report_on_save = False

    def execute(self):
        """
        Perform all the task needed for the Feature Engineering to complete.
        """

        log.info("Execute Feature Engineering")
        log.debug("FeatureEngineering.execute()")

        if len(sys.argv) > 1:
            self.parse_arguments()
        self.load()
        self.process()
        self.save()
        self.log_changes()
