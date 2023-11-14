import logging

from pathlib import Path

STD_FORMAT = '[%(levelname)s] %(asctime)s - %(name)s:%(message)s'


def configure_logging(log_file=None, log_level=logging.WARN):
    """
    Configure logging with a file handler and a console handler
    :param log_file: file, with full path, to create log entries in
    :param log_level: The logging level to show
    :return:
    """
    # Check if a root logger has handlers
    if not logging.root.handlers:
        # Create a formatter
        formatter = logging.Formatter(STD_FORMAT)

        # Get the root logger and add the handlers
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        if log_file is not None:
            # Create a file handler and set the formatter
            if not Path('/'.join(log_file.split('/')[:-1])).exists():
                raise Exception("An invalid path was specified for the log file.")
            file_handler = logging.FileHandler(log_file)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)

        # Create a console handler and set the formatter
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)


def get_logger(name=None):
    """
    Get a logger with the specified name or uses the calling function's name
    :param name:
    :return:
    """
    if name is None:
        # Get the name of the calling function
        frame = logging.currentframe()
        calling_module = frame.f_back.f_globals['__name__']
        name = calling_module

    return logging.getLogger(name)