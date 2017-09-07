# coding: utf-8

from pylons import config

from ckan import plugins as p

import oaipmh.common
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
from ckanext.oaipmh.interfaces import IOAIPMHHarvester

# for debug
import logging
log = logging.getLogger(__name__)


class DataCiteReader(object):
    """ Reader for DataCite XML data """


    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(DataCiteReader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`DataCiteReader.read`. """
        return self.read(xml)


    def read(self, xml):
        """ Extract package data from given XML.
        :param xml: xml element (lxml)
        :return: oaipmh.common.Metadata object generated from xml
        """
        from ckanext.oaipmh.datacite import DataCiteHarvester

        for harvester in p.PluginImplementations(IOAIPMHHarvester):
            package_dict = harvester.get_oaipmh_package_dict(DataCiteHarvester.md_format, xml)

        result = generic_xml_metadata_reader(xml).getMap()
        result['package_dict'] = package_dict
        return oaipmh.common.Metadata(xml, result)

