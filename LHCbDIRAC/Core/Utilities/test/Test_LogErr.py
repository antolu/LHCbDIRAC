# Test for LogErr.py

import unittest
import json
import ast
import os

from LHCbDIRAC.Core.Utilities.LogErr import create_json_table
from DIRAC import gLogger


class LogErrTestCase(unittest.TestCase):
  def __init__(self, *args, **kwargs):
    super(LogErrTestCase, self).__init__(*args, **kwargs)

  def setUp(self):

    # Define test data
    self.jsonDataMultiple = [
        {
            'G4Exception': [
                {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                        'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}]}, {
            'ERROR Gap not found!': [
                {
                    'runnr': '  Run 133703', 'eventnr': 'Evt 29'}]}, {
            'The signal decay mode is not defined in the main DECAY.DEC table': [
                {
                    'runnr': '  Run 133703', 'eventnr': 'Evt 29'}, {
                    'runnr': '  Run 123', 'eventnr': 'Evt 30'}]}, {
            'G4Exception : StuckTrack': [
                {
                    'runnr': '  Run 133703', 'eventnr': 'Evt 29'}, {
                    'runnr': '  Run 123', 'eventnr': 'Evt 30'}, {
                    'runnr': '  Run 1234', 'eventnr': 'Evt 31'}]}, {
            'G4Exception : 001': [
                {
                    'runnr': '  Run 133703', 'eventnr': 'Evt 29'}, {
                    'runnr': '  Run 123', 'eventnr': 'Evt 30'}, {
                    'runnr': '  Run 1234', 'eventnr': 'Evt 31'}]}]
    self.jsonDataSingle = [
        {
            'G4Exception : InvalidSetup': [
                {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}, {
                        'runnr': '', 'eventnr': ''}, {
                    'runnr': '', 'eventnr': ''}]}]
    self.jsonDataEmpty = []

    # Define name of output file
    self.name = 'test_create_JSON_table.json'

  def tearDown(self):

    # Remove file
    os.remove(self.name)

    self.jsonDataMultiple = None
    self.jsonDataSingle = None
    self.jsonDataEmpty = None

    self.name = None


class TestLogErr(LogErrTestCase):
  def test_createJsonMultiple(self):

    expected = json.dumps(
        {
            "Errors": {
                "ERROR Gap not found!": 1,
                "ID": {
                    "wmsID": "5",
                    "ProductionID": "4",
                    "JobID": "3"
                },
                "G4Exception": 10,
                "The signal decay mode is not defined in the main DECAY.DEC table": 2,
                "G4Exception : StuckTrack": 3,
                "G4Exception : 001": 3
            }
        }, indent=2)

    create_json_table(self.jsonDataMultiple, self.name, '3', '4', '5')
    with open(self.name, 'r') as f:
      file = f.read()

    # Convert to dict()
    file = ast.literal_eval(file)
    expected = ast.literal_eval(expected)

    self.assertEqual(file, expected)

  def test_createJsonSingle(self):

    expected = json.dumps(
        {
            "Errors": {
                "G4Exception : InvalidSetup": 10,
                "ID": {
                    "wmsID": "5",
                    "ProductionID": "4",
                    "JobID": "3"
                }
            }
        }, indent=2)

    create_json_table(self.jsonDataSingle, self.name, '3', '4', '5')
    with open(self.name, 'r') as f:
      file = f.read()

    file = ast.literal_eval(file)
    expected = ast.literal_eval(expected)

    self.assertEqual(file, expected)

  def test_createJsonEmpty(self):

    expected = json.dumps(
        {
            "Errors": {
                "ID": {
                    "wmsID": "5",
                    "ProductionID": "4",
                    "JobID": "3"
                }
            }
        }, indent=2)

    create_json_table(self.jsonDataEmpty, self.name, '3', '4', '5')
    with open(self.name, 'r') as f:
      file = f.read()

    # Convert to dict()
    file = ast.literal_eval(file)
    expected = ast.literal_eval(expected)

    self.assertEqual(file, expected)


if __name__ == '__main__':
  suite = unittest.defaultTestLoader.loadTestsFromTestCase(LogErrTestCase)
  suite.addTest(unittest.defaultTestLoader.loadTestsFromTestCase(TestLogErr))
  testResult = unittest.TextTestRunner(verbosity=2).run(suite)
