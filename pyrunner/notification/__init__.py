import sys
from pyrunner.core import config
from .email import EmailNotification
from .abstract import Notification


#def get_notification_instance(class_name=config["notification"], **kwargs):
#    c = getattr(sys.modules[__name__], class_name)
#    return c(**kwargs)
