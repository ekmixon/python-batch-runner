import sys
from pyrunner.core.config import config
from .list import ListFileJobSpec
from .json import JsonFileJobSpec
from .abstract import JobSpec


def get_serde_instance(class_name=config["components"]["serde"], **kwargs):
    c = getattr(sys.modules[__name__], class_name)
    return c(**kwargs)
