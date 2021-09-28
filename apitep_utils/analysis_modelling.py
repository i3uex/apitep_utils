import logging
import sys
from apitep_utils.data_processor import DataProcessor

log = logging.getLogger(__name__)


class AnalysisModeling(DataProcessor):
    save_report_on_load = False
    save_report_on_save = False
    model_developed = None

    def analise(self):
        """
        Analyse the data loaded in the transformation.

        Make sure to use `input_df` as the input of your pipeline.
        """

        log.info("Process dataset")
        log.debug("AnalysisModelling.analise()")

        raise NotImplementedError

    def save(self):
        """
        Save the result of the analysis.
        """

        log.info("Save analysis result")
        log.debug("AnalysisModelling.save()")

        raise NotImplementedError

    def execute(self):
        """
        Perform all the task needed for the AnalysisModeling to complete.
        """

        log.info("Execute Analysis and Modeling")
        log.debug("AnalysisModeling.execute()")

        if len(sys.argv) > 1:
            self.parse_arguments()
        self.load()
        self.process()
        self.analise()
        self.save()
        self.log_changes()

