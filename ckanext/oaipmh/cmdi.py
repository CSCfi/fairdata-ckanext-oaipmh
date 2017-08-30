import json
import logging
from lxml import etree
import oaipmh
from ckanext.oaipmh.cmdi_reader import CmdiReader
from ckanext.oaipmh.harvester import OAIPMHHarvester

from ckanext.etsin.data_catalog_service import get_data_catalog_id
import ckanext.oaipmh.utils as utils

log = logging.getLogger(__name__)


class CMDIHarvester(OAIPMHHarvester):
    md_format = 'cmdi0571'
    client = None  # used for testing

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'cmdi',
            'title': 'OAI-PMH CMDI',
            'description': 'Harvests CMDI dataset'
        }

    def on_deleted(self, harvest_object, header):
        """ See :meth:`OAIPMHHarvester.on_deleted`
            Mark package for deletion.
        """
        package_id = utils.get_package_id_by_pid(header.identifier(), 'primary')
        if package_id:
            harvest_object.package_id = package_id
        harvest_object.content = None
        harvest_object.report_status = "deleted"
        harvest_object.save()
        return True

    def gather_stage(self, harvest_job):
        """ See :meth:`OAIPMHHarvester.gather_stage`  """

        config = self._get_configuration(harvest_job)
        if not config.get('type'):
            config['type'] = 'cmdi'
            harvest_job.source.config = json.dumps(config)
            harvest_job.source.save()
        registry = self.metadata_registry(config, harvest_job)
        client = self.client or oaipmh.client.Client(harvest_job.source.url, registry)

        # Get data catalog id to be used in harvest_object
        # so that import_stage can access it
        return self.populate_harvest_job(harvest_job, None, config, client, get_data_catalog_id(config))

    def parse_xml(self, f, context, orig_url=None, strict=True):
        return CmdiReader().read_data(etree.fromstring(f))
