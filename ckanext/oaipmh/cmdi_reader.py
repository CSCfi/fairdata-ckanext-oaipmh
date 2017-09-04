import oaipmh.common
from pylons import config
from ckanext.oaipmh.importcore import generic_xml_metadata_reader
from ckanext.etsin.mappers.cmdi import cmdi_mapper


class CmdiReader(object):
    """ Reader for CMDI XML data """

    namespaces = {'oai': "http://www.openarchives.org/OAI/2.0/", 'cmd': "http://www.clarin.eu/cmd/"}
    LICENSE_CLARIN_PUB = "CLARIN_PUB"
    LICENSE_CLARIN_ACA = "CLARIN_ACA"
    LICENSE_CLARIN_RES = "CLARIN_RES"
    LICENSE_CC_BY = "CC-BY"
    PID_PREFIX_URN = "urn.fi"

    def __init__(self, provider=None):
        """ Generate new reader instance.
        :param provider: URL used for pids.
        """
        super(CmdiReader, self).__init__()
        self.provider = provider or config.get('ckan.site_url')

    def __call__(self, xml):
        """ Call :meth:`CmdiReader.read`. """
        return self.read(xml)

    def read(self, xml):
        """ Extract package data from given XML.
        :param xml: xml element (lxml)
        :return: oaipmh.common.Metadata object generated from xml
        """
        result = generic_xml_metadata_reader(xml).getMap()
        result['package_dict'] = cmdi_mapper(xml)
        return oaipmh.common.Metadata(xml, result)
