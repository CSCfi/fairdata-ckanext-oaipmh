# This file is part of the Etsin harvester service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: GNU Affero General Public License version 3

from ckan.plugins.interfaces import Interface


class IOAIPMHHarvester(Interface):

    def get_oaipmh_package_dict(self, format, etree_xml):
        '''
        Allows to modify the dataset dict that will be created or updated

        This is the dict that the harvesters will pass to the `package_create`
        or `package_update` actions. Extensions can modify it to suit their
        needs, adding or removing filds, modifying the default ones, etc.

        This method should always return a package_dict. Note that, although
        unlikely in a particular instance, this method could be implemented by
        more than one plugin.

        If a dict is not returned by this function, the import stage will be
        cancelled.


        :param context: Contains a reference to the model, eg to
                        perform DB queries, and the user name used for
                        authorization.
        :type context: dict
        :param format: Format of oaipmh harvester source (e.g. cmdi)
        :param etree_xml: Source xml data in etree.

        :returns: A dataset dict ready to be used by ``package_create`` or
                  ``package_update``
        :rtype: dict
        '''
        return {}
