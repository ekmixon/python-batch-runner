import sys
from pyrunner.core import config
from .list import ListSerDe
from .json import JsonSerDe
from .abstract import SerDe


#def get_serde_instance(class_name=config["serde"], **kwargs):
#    c = getattr(sys.modules[__name__], class_name)
#    return c(**kwargs)
