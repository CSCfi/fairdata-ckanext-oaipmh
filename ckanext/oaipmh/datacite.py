# This file is part of the Etsin harvester service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: GNU Affero General Public License version 3

import json
import logging
import oaipmh

from ckan import model
from ckanext.oaipmh.harvester import OAIPMHHarvester

log = logging.getLogger(__name__)

class DataCiteHarvester(OAIPMHHarvester):
    md_format = 'oai_datacite' # 'cmdi0571'
    client = None  # used for testing

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'datacite',
            'title': 'OAI-PMH DataCite',
            'description': 'Harvests DataCite v.3.1 datasets'
        }
