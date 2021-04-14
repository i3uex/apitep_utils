import logging
import sys
from typing import List

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
    changes: List

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

    def log_changes(self):
        """
        Dump to log how many changes are made to UEx dataset.
        """

        log.info("Log dataset changes")
        log.debug("log_changes()")

        for key in self.changes:
            log.info(f"- {key}: {self.changes[key]}")

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
