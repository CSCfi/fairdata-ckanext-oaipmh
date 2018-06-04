# coding: utf-8
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name
#
# This file is part of the Etsin harvester service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: GNU Affero General Public License version 3

"""
Unit tests for OAI-PMH harvester.
"""

from unittest import TestCase
from lxml import etree
from pylons import config
import ckan
from ckanext.harvest.model import HarvestJob, HarvestSource, HarvestObject
from ckanext.oaipmh.cmdi import CMDIHarvester
from ckanext.oaipmh.cmdi_reader import CmdiReader
import ckanext.harvest.model as harvest_model
import ckanext.kata.model as kata_model
import os
from ckan import model
from ckan.logic import get_action
import json
import ckanext.kata.utils as utils

def _get_fixture(filename):
    return os.path.join(os.path.dirname(__file__), "..", "test_fixtures", filename)


def _get_record(filename):
    tree = etree.parse(_get_fixture(filename))
    return tree.xpath('/oai:OAI-PMH/*/oai:record', namespaces={'oai': 'http://www.openarchives.org/OAI/2.0/'})[0]


class _FakeIdentifier():
    def __init__(self, identifier):
        self._identifier = identifier

    def identifier(self):
        return self._identifier


class _FakeClient():
    def listIdentifiers(self, metadataPrefix):
        return [_FakeIdentifier('oai:kielipankki.fi:sha3a880')]


class TestCMDIHarvester(TestCase):
    @classmethod
    def setup_class(cls):
        ''' Setup database and variables '''
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = CMDIHarvester()

    def tearDown(self):
        """ rebuild database """
        ckan.model.repo.rebuild_db()

    def _run_import(self, xml, job):
        if not model.User.get('harvest'):
            model.User(name='harvest', sysadmin=True).save()
        if not model.Group.get('test'):
            get_action('organization_create')({'user': 'harvest'}, {'name': 'test'})

        record = _get_record(xml)

        metadata = CmdiReader()(record)
        metadata['package_dict']['owner_org'] = "test"

        harvest_object = HarvestObject()
        harvest_object.content = json.dumps(metadata.getMap())
        harvest_object.id = xml
        harvest_object.guid = xml
        harvest_object.source = job.source
        harvest_object.harvest_source_id = None
        harvest_object.job = job
        harvest_object.save()

        self.harvester.import_stage(harvest_object)
        return harvest_object

    def test_reader(self):
        record = _get_record("cmdi_1.xml")
        metadata = CmdiReader("http://localhost/test")(record)
        content= metadata.getMap()
        package = content['package_dict']
        self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')
        self.assertEquals(package.get('title', []), '{"eng": "Longi Corpus"}')

    def test_gather(self):
        source = HarvestSource(url="http://localhost/test_cmdi", type="cmdi")
        source.save()
        job = HarvestJob(source=source)
        job.save()
        self.harvester.client = _FakeClient()
        self.harvester.gather_stage(job)

    def test_import(self):
        source = HarvestSource(url="http://localhost/test_cmdi", type="cmdi")
        source.save()
        job = HarvestJob(source=source)
        job.save()

        harvest_object = self._run_import("cmdi_1.xml", job)
        package_id = json.loads(harvest_object.content)['package_dict']['id']

        self.assertEquals(len(harvest_object.errors), 0, u"\n".join(unicode(error.message) for error in (harvest_object.errors or [])))

        package = get_action('package_show')({'user': 'harvest'}, {'id': package_id})

        self.assertEquals(package.get('notes', None), u'{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')
        self.assertEquals(package.get('title', []), '{"eng": "Longi Corpus"}')
        self.assertEquals(package.get('license_id', None), 'undernegotiation')

        provider = config['ckan.site_url']
        expected_pid = {u'id': u'http://islrn.org/resources/248-895-085-557-0',
                        u'provider': provider,
                        u'type': u'relation',
                        u'relation': u'generalRelation'}

        self.assertTrue(expected_pid not in package.get('pids'))

        model.Session.flush()

        harvest_object = self._run_import("cmdi_2.xml", job)
        package_id = json.loads(harvest_object.content)['package_dict']['id']

        self.assertEquals(len(harvest_object.errors), 0, u"\n".join(unicode(error.message) for error in (harvest_object.errors or [])))

        package = get_action('package_show')({'user': 'harvest'}, {'id': package_id})

        self.assertEquals(package['temporal_coverage_begin'], '1880')
        self.assertEquals(package['temporal_coverage_end'], '1939')
        self.assertEquals(package.get('license_id', None), 'other')
        # Delete package
        harvest_object = HarvestObject()
        harvest_object.content = None
        harvest_object.id = "test-cmdi-delete"
        harvest_object.guid = "test-cmdi-delete"
        harvest_object.source = job.source
        harvest_object.harvest_source_id = None
        harvest_object.job = job
        harvest_object.package_id = package.get('id')
        harvest_object.report_status = "deleted"
        harvest_object.save()

        self.harvester.import_stage(harvest_object)

        model.Session.flush()
        self.assertEquals(model.Package.get(package['id']).state, 'deleted')

    def test_fetch_xml(self):
        package = self.harvester.fetch_xml("file://%s" % _get_fixture('cmdi_1.xml'), {})
        self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
        self.assertEquals(package.get('version', None), '2012-09-07')

    def test_parse_xml(self):
        with open(_get_fixture('cmdi_1.xml'), 'r') as source:
            package = self.harvester.parse_xml(source.read(), {})
            self.assertEquals(package.get('notes', None), '{"eng": "Test description"}')
            self.assertEquals(package.get('version', None), '2012-09-07')
