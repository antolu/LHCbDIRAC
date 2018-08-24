"""
Test LogErrXML.py
"""

import json
import unittest
import ast
import xml.etree.ElementTree as ET

from DIRAC import gLogger
import LHCbDIRAC.Core.Utilities.LogErrXML as LogErrXML


class LogErrXMLTestCase(unittest.TestCase):

  def __init__(self, *args, **kwargs):
    super(LogErrXMLTestCase, self).__init__(*args, **kwargs)

  def setUp(self):
    self.jobID = 1
    self.prodID = 2
    self.wmsID = 3

  def tearDown(self):
    pass


class TestLogErrXML(LogErrXMLTestCase):

  #########################################
  # Test extractData()

  def test_extractDataWithNoCounter(self):
    rootNoCounter = ET.fromstring("<counters></counters>")
    expected = dict({"Counters": {"ID": {"JobID": 1, "ProductionID": 2, "wmsID": 3}}})
    result = LogErrXML.extractData(rootNoCounter, self.jobID, self.prodID, self.wmsID)

    self.assertTrue(result['OK'])
    self.assertEqual(ast.literal_eval(result['Value']), expected)

  def test_extractDataWithStringValue(self):
    rootStringValue = ET.fromstring('<counters><counter name = "test">"string"</counter></counters>')
    result = LogErrXML.extractData(rootStringValue, self.jobID, self.prodID, self.wmsID)

    self.assertFalse(result['OK'])

  def test_extractDataWithFloatValue(self):
    rootFloatValues = ET.fromstring('<counters><counter name = "test">1.5</counter></counters>')
    result = LogErrXML.extractData(rootFloatValues, self.jobID, self.prodID, self.wmsID)

    self.assertFalse(result['OK'])

  def test_extractDataNormal(self):
    root = ET.fromstring('<counters><counter name = "test1">1</counter><counter name = "test2">-1</counter></counters>')
    expected = dict({"Counters": {"test1": 1, "test2": -1, "ID": {"JobID": 1, "ProductionID": 2, "wmsID": 3}}})
    result = LogErrXML.extractData(root, self.jobID, self.prodID, self.wmsID)

    self.assertTrue(result['OK'])
    self.assertEqual(ast.literal_eval(result['Value']), expected)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(LogErrXMLTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestLogErrXML))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
