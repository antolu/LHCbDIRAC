"""
Test XMLErr.py
"""

import json
import unittest
import ast
import xml.etree.ElementTree as ET

from DIRAC import gLogger
import LHCbDIRAC.Core.Utilities.XMLErr as XMLErr


class XMLErrTestCase(unittest.TestCase):

  def __init__(self, *args, **kwargs):
    super(XMLErrTestCase, self).__init__(*args, **kwargs)

  def setUp(self):
    self.jobID = 1
    self.prodID = 2
    self.wmsID = 3

  def tearDown(self):
    pass


class TestXMLErr(XMLErrTestCase):

  #########################################
  # Test extractData()

  def test_extractDataWithNoCounter(self):
    rootNoCounter = ET.fromstring("<counters></counters>")
    expected = dict({"Counters": {"ID": {"JobID": 1, "ProductionID": 2, "wmsID": 3}}})
    result = XMLErr.extractData(rootNoCounter, self.jobID, self.prodID, self.wmsID)

    self.assertTrue(result['OK'])
    self.assertEqual(ast.literal_eval(result['Value']), expected)

  def test_extractDataWithStringValue(self):
    rootStringValue = ET.fromstring('<counters><counter name = "test">"string"</counter></counters>')
    result = XMLErr.extractData(rootStringValue, self.jobID, self.prodID, self.wmsID)

    self.assertFalse(result['OK'])

  def test_extractDataWithFloatValue(self):
    rootFloatValues = ET.fromstring('<counters><counter name = "test">1.5</counter></counters>')
    result = XMLErr.extractData(rootFloatValues, self.jobID, self.prodID, self.wmsID)

    self.assertFalse(result['OK'])

  def test_extractDataNormal(self):
    root = ET.fromstring('<counters><counter name = "test1">1</counter><counter name = "test2">-1</counter></counters>')
    expected = dict({"Counters": {"test1": 1, "test2": -1, "ID": {"JobID": 1, "ProductionID": 2, "wmsID": 3}}})
    result = XMLErr.extractData(root, self.jobID, self.prodID, self.wmsID)

    self.assertTrue(result['OK'])
    self.assertEqual(ast.literal_eval(result['Value']), expected)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(XMLErrTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestXMLErr))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)