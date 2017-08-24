import httplib
import json
import logging
import urllib2
from lxml import etree
import oaipmh
from ckanext.kata.utils import get_package_id_by_pid
from ckanext.oaipmh import importformats
from ckanext.oaipmh.cmdi_reader import CmdiReader
from ckanext.oaipmh.harvester import OAIPMHHarvester

from ckanext.etsin.data_catalog_service import DataCatalogMetaxAPIService

log = logging.getLogger(__name__)


class CMDIHarvester(OAIPMHHarvester):
    md_format = 'cmdi0571'
    client = None  # used for testing

    DATA_CATALOG_JSON_FILE_PATH = "resources/language_bank_data_catalog.json"

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'cmdi',
            'title': 'OAI-PMH CMDI',
            'description': 'Harvests CMDI dataset'
        }

    def get_schema(self, config, pkg):
        from ckanext.kata.plugin import KataPlugin
        return KataPlugin.create_package_schema_oai_cmdi()

    def on_deleted(self, harvest_object, header):
        """ See :meth:`OAIPMHHarvester.on_deleted`
            Mark package for deletion.
        """
        package_id = get_package_id_by_pid(header.identifier(), 'primary')
        if package_id:
            harvest_object.package_id = package_id
        harvest_object.content = None
        harvest_object.report_status = "deleted"
        harvest_object.save()
        return True

    def gather_stage(self, harvest_job):
        """ See :meth:`OAIPMHHarvester.gather_stage`  """

        # Get data catalog id to be used in harvest_object
        # so that import_stage can access it
        catalog_id = self._get_data_catalog_id()

        config = self._get_configuration(harvest_job)
        if not config.get('type'):
            config['type'] = 'cmdi'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
        registry = self.metadata_registry(config, harvest_job)
        client = self.client or oaipmh.client.Client(harvest_job.source.url, registry)
        return self.populate_harvest_job(harvest_job, None, config, client, catalog_id)

    def parse_xml(self, f, context, orig_url=None, strict=True):
        return CmdiReader().read_data(etree.fromstring(f))

    def _get_data_catalog_id(self):
        catalog_service = DataCatalogMetaxAPIService()
        return catalog_service.create_or_update_data_catalogs(True, CMDIHarvester.DATA_CATALOG_JSON_FILE_PATH)
