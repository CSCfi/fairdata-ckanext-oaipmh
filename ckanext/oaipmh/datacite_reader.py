# coding: utf-8
#
# This file is part of the Etsin harvester service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: GNU Affero General Public License version 3

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

        package_dict = {}

        for harvester in p.PluginImplementations(IOAIPMHHarvester):
            package_dict = harvester.get_oaipmh_package_dict(DataCiteHarvester.md_format, xml)

        return oaipmh.common.Metadata(xml, package_dict)

