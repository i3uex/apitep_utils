import os


class Path:

    @staticmethod
    def add_prefix_to_file_name(prefix: str, path: str):
        directory = os.path.dirname(path)
        basename = os.path.basename(path)
        basename_parts = os.path.splitext(basename)
        file_name = basename_parts[0]
        file_extension = basename_parts[1]
        file_name_with_prefix = f"{prefix}{file_name}{file_extension}"

        return os.path.join(directory, file_name_with_prefix)

    @staticmethod
    def add_suffix_to_file_name(suffix: str, path: str):
        directory = os.path.dirname(path)
        basename = os.path.basename(path)
        basename_parts = os.path.splitext(basename)
        file_name = basename_parts[0]
        file_extension = basename_parts[1]
        file_name_with_suffix = f"{file_name}{suffix}{file_extension}"

        return os.path.join(directory, file_name_with_suffix)
