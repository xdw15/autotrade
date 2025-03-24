from libs.execution_handler import *
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_Formatter = logging.Formatter(
    fmt='{levelname:<10}---{name:<30}--{filename:<30}--{thread:<10}---{message:<40}--{asctime:12}',
    datefmt='%m/%d-%H:%M:%S',
    style='{',
    validate=True
)

log_stream_handler = logging.StreamHandler()
log_stream_handler.setFormatter(log_Formatter)
logger.addHandler(log_stream_handler)

auto_exec = AutoExecution()

auto_exec.update_system()
