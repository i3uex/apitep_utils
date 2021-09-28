import logging
from datetime import datetime


class Date:
    DEFAULT_FORMAT = "%Y-%m-%d"

    @staticmethod
    def change_format(date: str, input_format: str, output_format: str = DEFAULT_FORMAT) -> str:
        """
        Change date format, using a hyphen as separator instead of slash. Date
        parts order is year-month-day, instead of day-month-year.

        :param date: string representing the date.
        :param input_format: string describing the input format.
        :param output_format: string describing the output format.
        :return: string representing the date with its format changed.
        :rtype: str
        """
        # logging.info(f"Change date format")
        # logging.debug(f"Date.change_format("
        #               f"date={date}, ")
        #               f"input_format={input_format}, ")
        #               f"output_format={output_format})")

        if date == "" or input_format == "" or output_format == "":
            return ""
        else:
            input_date = datetime.strptime(date, input_format)
            output_date = input_date.strftime(output_format)
            return str(output_date)
