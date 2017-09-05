import logging
import json

import oaipmh.client
import oaipmh.error

import importformats

from ckan.model import Session, Package
from ckan import model
from ckan import plugins as p
from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestObjectExtra as HOExtra
from ckanext.harvest.harvesters.base import HarvesterBase

import fnmatch
import re
import uuid
import dateutil

log = logging.getLogger(__name__)


class OAIPMHHarvester(HarvesterBase):
    '''
    OAI-PMH Harvester
    '''

    def _get_configuration(self, harvest_job):
        """ Parse configuration from given harvest object """
        configuration = {}
        if harvest_job.source.config:
            log.debug('Config: %s', harvest_job.source.config)
            try:
                configuration = json.loads(harvest_job.source.config)
            except ValueError as e:
                self._save_gather_error('Gather: Unable to decode config from: {c}, {e}'.
                                        format(e=e, c=harvest_job.source.config), harvest_job)
                raise
        return configuration

    def metadata_registry(self, config, harvest_job):
        harvest_type = config.get('type', 'default')
        return importformats.create_metadata_registry(harvest_type, harvest_job.source.url)

    def validate_config(self, config):
        '''
        Harvesters can provide this method to validate the configuration entered in the
        form. It should return a single string, which will be stored in the database.
        Exceptions raised will be shown in the form's error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

        # TODO: Tests

        def validate_param(d, p, t):
            '''
            Check if 'p' is specified and is of type 't'
            '''
            if p in d and not isinstance(d[p], t):
                raise TypeError("'{p}' needs to be a '{t}'".format(t=t, p=p))
            return p in d

        # Todo: Write better try/except cases
        if config:
            dj = json.loads(config)
            validate_param(dj, 'set', list)
            validate_param(dj, 'type', basestring)
        else:
            config = '{}'
        return config

    def get_record_identifiers(self, set_ids, config, client):
        ''' Get package identifiers from given set identifiers.
        '''

        kwargs = dict(config.items())
        kwargs.pop('data_catalog_id')
        kwargs['metadataPrefix'] = self.md_format
        if set_ids:
            for set_id in set_ids:
                try:
                    for header in client.listIdentifiers(set=set_id, **kwargs):
                        yield header.identifier()
                except oaipmh.error.NoRecordsMatchError:
                    pass
        else:
            try:
                for header in client.listIdentifiers(**kwargs):
                    yield header.identifier()
            except oaipmh.error.NoRecordsMatchError:
                pass

    def populate_harvest_job(self, harvest_job, set_ids, config, client):
        # Check if this source has been harvested before
        previous_job = Session.query(HarvestJob) \
            .filter(HarvestJob.source == harvest_job.source) \
            .filter(HarvestJob.gather_finished != None) \
            .filter(HarvestJob.id != harvest_job.id) \
            .order_by(HarvestJob.gather_finished.desc()) \
            .limit(1).first()

        # Collect record identifiers
        record_identifiers = list(self.get_record_identifiers(set_ids, config, client))
        log.debug('Record identifiers: %s', record_identifiers)

        if previous_job:
            for previous_error in [error.guid for error in Session.query(HarvestObject).
                                   filter(HarvestObject.harvest_job_id == previous_job.id).
                                   filter(HarvestObject.state == 'ERROR').all()]:
                if previous_error not in record_identifiers:
                    record_identifiers.append(previous_error)
        try:
            object_ids = []
            if len(record_identifiers):

                query = model.Session.query(HarvestObject.guid, HarvestObject.package_id). \
                    filter(HarvestObject.current == True). \
                    filter(HarvestObject.harvest_source_id == harvest_job.source.id)

                db_guid_to_package_id = {}
                for ho_in_db_guid, ho_in_db_package_id in query:
                    db_guid_to_package_id[ho_in_db_guid] = ho_in_db_package_id

                guids_in_db = set(db_guid_to_package_id.keys())
                guids_in_harvest = set(record_identifiers)

                new = guids_in_harvest - guids_in_db
                delete = guids_in_db - guids_in_harvest
                change = guids_in_db & guids_in_harvest

                for guid in new:
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        extras=[HOExtra(key='status', value='new')])
                    obj.save()
                    object_ids.append(obj.id)
                for guid in change:
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        package_id=db_guid_to_package_id[guid],
                                        extras=[HOExtra(key='status', value='change')])
                    obj.save()
                    object_ids.append(obj.id)
                for guid in delete:
                    obj = HarvestObject(guid=guid, job=harvest_job,
                                        package_id=db_guid_to_package_id[guid],
                                        extras=[HOExtra(key='status', value='delete')])
                    model.Session.query(HarvestObject). \
                        filter_by(guid=guid). \
                        update({'current': False}, False)
                    obj.save()
                    object_ids.append(obj.id)

                log.debug('Object ids: {i}'.format(i=object_ids))
                return object_ids
            else:
                self._save_gather_error('No packages received for URL: {u}'.format(
                    u=harvest_job.source.url), harvest_job)
                return None
        except Exception as e:
            self._save_gather_error('Gather: {e}'.format(e=e), harvest_job)
            raise

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
        - gathering all the necessary objects to fetch on a later.
        stage (e.g. for a CSW server, perform a GetRecords request)
        - creating the necessary HarvestObjects in the database, specifying
        the guid and a reference to its job. The HarvestObjects need a
        reference date with the last modified date for the resource, this
        may need to be set in a different stage depending on the type of
        source.
        - creating and storing any suitable HarvestGatherErrors that may
        occur.
        - returning a list with all the ids of the created HarvestObjects.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        :type harvest_job: HarvestJob
        '''
        log.debug('Starting gather stage')
        log.debug('Harvest source: %s', harvest_job.source.url)

        config = self._get_configuration(harvest_job)

        # Create a OAI-PMH Client
        registry = self.metadata_registry(config, harvest_job)
        client = oaipmh.client.Client(harvest_job.source.url, registry)

        available_sets = list(client.listSets())

        log.debug('Available sets: %s', available_sets)

        set_ids = set()
        for set_id in config.get('set', []):
            if '*' in set_id:
                matcher = re.compile(fnmatch.translate(set_id))
                found = False
                for set_spec, _, _ in available_sets:
                    if matcher.match(set_spec):
                        set_ids.add(set_spec)
                        found = True
                if not found:
                    log.warning("No sets found with given wildcard string: %s", set_id)
            else:
                if not any(set_id in sets for sets in available_sets):
                    log.warning("Given set %s is not in available sets. Not removing.", set_id)
                set_ids.add(set_id)

        if len(set_ids):
            log.debug('Sets in config: %s', set_ids)
        return self.populate_harvest_job(harvest_job, set_ids, config, client)

    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
        - getting the contents of the remote object (e.g. for a CSW server, perform a GetRecordById request).
        - saving the content in the provided HarvestObject.
        - creating and storing any suitable HarvestObjectErrors that may occur.
        - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''
        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
        - performing any necessary action with the fetched object (e.g create a CKAN package).
        Note: if this stage creates or updates a package, a reference
        to the package should be added to the HarvestObject.
        - creating the HarvestObject
        - Package relation (if necessary)
        - creating and storing any suitable HarvestObjectErrors that may occur.
        - returning True if everything went as expected, False otherwise.

        :param harvest_object: HarvestObject object
        :returns: True if everything went right, False if errors were found
        '''

        log.debug('Import stage for harvest object with guid: %s', harvest_object.guid)

        if not harvest_object:
            log.error('No harvest object received')
            return False

        config = self._get_configuration(harvest_object)

        context = {
            'model': model,
            'session': model.Session,
            'user': 'harvest',
        }

        status = self._get_object_extra(harvest_object, 'status')

        # Get metadata content from provider
        try:
            # Create a OAI-PMH Client and get record
            registry = self.metadata_registry(config, harvest_object)
            client = oaipmh.client.Client(harvest_object.job.source.url, registry)
            header, metadata, _about = client.getRecord(identifier=harvest_object.guid,
                                                        metadataPrefix=self.md_format)
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Unable to get metadata from provider: {u}: {e}'.format(
                u=harvest_object.source.url, e=e), harvest_object)
            return False

        if header and header.datestamp():
            harvest_object.metadata_modified_date = header.datestamp()
            harvest_object.save()

        # Get contents
        try:
            content = json.dumps(metadata.getMap())
            # Save the fetched contents in the HarvestObject
            harvest_object.content = content
            harvest_object.save()
        except Exception as e:
            import traceback
            traceback.print_exc()
            self._save_object_error('Unable to get content for package: {u}: {e}'.format(
                u=harvest_object.source.url, e=e), harvest_object)
            return False

        if status == 'delete':
            # Delete package
            context.update({
                'ignore_auth': True,
            })
            p.toolkit.get_action('package_delete')(context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'.format(harvest_object.package_id, harvest_object.guid))
            return True

        if harvest_object.content is None:
            self._save_object_error('Empty content for object {0}'.format(harvest_object.id), harvest_object, 'Import')
            return False

        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.current == True) \
            .first()

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Get mapped package_dict
        content = json.loads(harvest_object.content)
        package_dict = content.pop('package_dict')
        context.update({'xml': metadata.element()})
        context.update({'return_id_only': True})

        # Set data catalog id to package_dict, if it exists in
        # harvest source configuration
        if config.get('data_catalog_id', False):
            package_dict['data_catalog'] = config.get('data_catalog_id')

        if status == 'new':
            package_dict['id'] = unicode(uuid.uuid4())
            try:
                package_id = p.toolkit.get_action('package_create')(context, package_dict)

                # Save reference to the package on the object
                harvest_object.package_id = package_id
                harvest_object.add()
                # Defer constraints and flush so the dataset can be indexed with
                # the harvest object id (on the after_show hook from the harvester
                # plugin)
                model.Session.execute('SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
                model.Session.flush()
                log.info('Created new package %s with guid %s', package_id, harvest_object.guid)
            except p.toolkit.ValidationError, e:
                self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                return False

        elif status == 'change':

            # Check if the modified date is more recent
            if previous_object and harvest_object.metadata_modified_date <= previous_object.metadata_modified_date:

                # Assign the previous job id to the new object to
                # avoid losing history
                harvest_object.harvest_job_id = previous_object.job.id
                harvest_object.add()

                # Delete the previous object to avoid cluttering the object table
                previous_object.delete()
                log.info('Document with GUID %s unchanged, skipping...' % (harvest_object.guid))
            else:
                package_dict['id'] = harvest_object.package_id
                try:
                    package_id = p.toolkit.get_action('package_update')(context, package_dict)
                    log.info('Updated package %s with guid %s', package_id, harvest_object.guid)
                except p.toolkit.ValidationError, e:
                    self._save_object_error('Validation Error: %s' % str(e.error_summary), harvest_object, 'Import')
                    return False

        model.Session.commit()

        return True