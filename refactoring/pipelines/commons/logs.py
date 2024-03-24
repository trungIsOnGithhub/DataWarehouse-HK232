import os
import logging, coloredlogs

file_handler_log_formatter = logging.Formatter(
    "%(asctime)s  |  %(levelname)s  |  %(message)s  "
)
console_handler_log_formatter = coloredlogs.ColoredFormatter(
    fmt="%(message)s",
    level_styles=dict(
        debug=dict(color="white"),
        info=dict(color="green"),
        warning=dict(color="cyan"),
        error=dict(color="red", bold=True, bright=True),
        critical=dict(color="black", bold=True, background="red"),
    ),
    field_styles=dict(messages=dict(color="white")),
)


def get_logger(layer_name, caller_filename, is_console=False):
    # return a logger customized for each script files

    root_logger = logging.getLogger(__name__)
    root_logger.setLevel(logging.DEBUG)

    if is_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(console_handler_log_formatter)

        root_logger.addHandler(console_handler)
    else:
        file_handler = logging.FileHandler(os.path.join('logs', layer_name, caller_filename), mode='w')
        file_handler.setFormatter(file_handler_log_formatter)

        root_logger.addHandler(file_handler)

    return root_logger