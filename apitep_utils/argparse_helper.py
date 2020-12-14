import logging
import os


class ArgumentParserHelper:

    @staticmethod
    def parse_data_file_path(data_file_path: str, check_is_file: bool = True) -> str:
        """
        Test if a file exists in the path provided, abort if not.

        :return: path provided if a file exists there.
        :rtype: str
        """
        logging.info(f"Check if path exists")
        logging.debug(f"ArgumentParserHelper.parse_data_file_path("
                      f"data_file_path={data_file_path})")

        if data_file_path == "":
            logging.error(f"Data file path cannot be empty")
            print(f"path to data file not provided")
            exit(1)

        if check_is_file:
            if not os.path.isfile(data_file_path):
                logging.error(f"Data file path \"{data_file_path}\" does not exist")
                print(f"cannot open data file {data_file_path}")
                exit(1)

        return data_file_path
