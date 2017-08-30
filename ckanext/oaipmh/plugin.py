import logging

from ckan.plugins import implements, SingletonPlugin


log = logging.getLogger(__name__)


class OAIPMHPlugin(SingletonPlugin):
    '''OAI-PMH harvesting plugin. This class name must be defined for extension to work.
    '''