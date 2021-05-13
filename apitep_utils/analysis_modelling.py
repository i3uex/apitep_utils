import logging

from apitep_utils.data_processor import DataProcessor

log = logging.getLogger(__name__)


class AnalysisModelling(DataProcessor):
    save_report_on_load = False
    save_report_on_save = False

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
