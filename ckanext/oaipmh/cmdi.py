from ckanext.oaipmh.harvester import OAIPMHHarvester

class CMDIHarvester(OAIPMHHarvester):
    md_format = 'cmdi0571'

    def info(self):
        ''' See ;meth:`ckanext.harvest.harvesters.base.HarvesterBase.info`. '''

        return {
            'name': 'cmdi',
            'title': 'OAI-PMH CMDI',
            'description': 'Harvests CMDI dataset'
        }