import logging

from apitep_utils.data_processor import DataProcessor

log = logging.getLogger(__name__)


class Transformation(DataProcessor):

    description: str = "Transformation"
