# coding: utf-8
#
# pylint: disable=no-self-use, missing-docstring, too-many-public-methods, invalid-name
"""
Functional tests for OAI-PMH harvester.
"""

import datetime
from unittest import TestCase

import oaipmh.client
import ckan
import random

from ckan.model import Group
from ckanext.harvest import model as harvest_model
from ckanext.oaipmh import importformats
from ckanext.oaipmh.harvester import OAIPMHHarvester
import ckanext.kata.model as kata_model
import ckanext.kata.utils as utils

from ckan.tests import WsgiAppCase
from ckan.lib.helpers import url_for

import lxml.etree
from ckan.logic import get_action
from ckan import model
from ckanext.kata.tests.test_fixtures.unflattened import TEST_DATADICT

from copy import deepcopy
import os

from pylons.util import AttribSafeContextObj, PylonsContext, pylons


FIXTURE_LISTIDENTIFIERS = "listidentifiers.xml"
FIXTURE_DATASET = "oai-pmh.xml"


def _get_fixture(filename):
    return "file://%s" % os.path.join(os.path.dirname(__file__), "..", "test_fixtures", filename)


class TestReadingFixtures(TestCase):

    TEST_ID = "urn:nbn:fi:csc-ida2013032600070s"

    @classmethod
    def setup_class(cls):
        '''
        Setup database and variables
        '''
        model.repo.rebuild_db()
        harvest_model.setup()
        kata_model.setup()
        cls.harvester = OAIPMHHarvester()

        # The Pylons globals are not available outside a request. This is a hack to provide context object.
        c = AttribSafeContextObj()
        py_obj = PylonsContext()
        py_obj.tmpl_context = c
        pylons.tmpl_context._push_object(c)

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()


    def test_fetch(self):
        '''
        Parse example dataset
        '''
        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(_get_fixture(FIXTURE_DATASET), registry)
        record = client.getRecord(identifier=self.TEST_ID, metadataPrefix='oai_dc')

        assert record

    def test_fetch_fail(self):
        '''
        Try to parse ListIdentifiers result as a dataset (basically testing PyOAI)
        '''
        def getrecord():
            client.getRecord(identifier=self.TEST_ID, metadataPrefix='oai_dc')

        registry = importformats.create_metadata_registry()
        client = oaipmh.client.Client(_get_fixture(FIXTURE_LISTIDENTIFIERS), registry)
        self.assertRaises(Exception, getrecord)