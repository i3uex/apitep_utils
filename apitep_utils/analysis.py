import logging

from apitep_utils.transformation import Transformation

log = logging.getLogger(__name__)


class Analysis(Transformation):

    def analise(self):
        """
        Analyse the data loaded in the transformation.

        Make sure to use `input_df` as the input of your pipeline.
        """

        log.info("Process dataset")
        log.debug("Analysis.analise()")

        raise NotImplementedError

    def save(self):
        """
        Save the result of the analysis.
        """

        log.info("Save analysis result")
        log.debug("Analysis.save()")

        raise NotImplementedError
