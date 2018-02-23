from ckan import plugins as p
import oaipmh.common
from pylons import config
from ckanext.oaipmh.interfaces import IOAIPMHHarvester


class Ddi25Reader(object):
    """ Reader for DDI 2.5 XML data """

    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(Ddi25Reader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`Ddi25Reader.read`. """
        return self.read(xml)

    def read(self, xml):
        """ Extract package data from given XML.
        :param xml: xml element (lxml)
        :return: oaipmh.common.Metadata object generated from xml
        """
        from ckanext.oaipmh.ddi25 import Ddi25Harvester

        package_dict = {}

        for harvester in p.PluginImplementations(IOAIPMHHarvester):
            package_dict = harvester.get_oaipmh_package_dict(Ddi25Harvester.md_format, xml)

        return oaipmh.common.Metadata(xml, package_dict)
