from ckanext.oaipmh.harvester import OAIPMHHarvester


class Ddi25Harvester(OAIPMHHarvester):
    md_format = 'ddi_c'

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'ddi25',
            'title': 'OAI-PMH DDI 2.5',
            'description': 'Harvests DDI 2.5 dataset'
        }