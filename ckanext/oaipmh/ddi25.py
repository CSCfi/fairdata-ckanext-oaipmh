# This file is part of the Etsin harvester service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: GNU Affero General Public License version 3

from ckanext.oaipmh.harvester import OAIPMHHarvester


class Ddi25Harvester(OAIPMHHarvester):
    md_format = 'oai_ddi25'

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'ddi25',
            'title': 'OAI-PMH DDI 2.5',
            'description': 'Harvests DDI 2.5 dataset'
        }