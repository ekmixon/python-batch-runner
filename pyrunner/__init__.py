from .core.pyrunner import PyRunner
from .worker.abstract import Worker
from .worker.shellworker import ShellWorker

from pyrunner.core.config import Config
from pyrunner.core.context import Context

config = Config()
context = Context()