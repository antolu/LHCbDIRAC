###############################################################################
# (c) Copyright 2019 CERN for the benefit of the LHCb Collaboration           #
#                                                                             #
# This software is distributed under the terms of the GNU General Public      #
# Licence version 3 (GPL Version 3), copied verbatim in the file "LICENSE".   #
#                                                                             #
# In applying this licence, CERN does not waive the privileges and immunities #
# granted to it by virtue of its status as an Intergovernmental Organization  #
# or submit itself to any jurisdiction.                                       #
###############################################################################
''' NagiosProbesCommand

  The Command gets information from the MonitoringTest cache.

'''

__RCSID__ = "$Id$"

from DIRAC import S_OK, S_ERROR
from DIRAC.ResourceStatusSystem.Command.Command import Command
from LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient


class NagiosProbesCommand(Command):

  def __init__(self, args=None, clients=None):

    super(NagiosProbesCommand, self).__init__(args, clients)

    if 'LHCbResourceManagementClient' in self.apis:
      self.rmClient = self.apis['ResourceManagementClient']
    else:
      self.rmClient = ResourceManagementClient()

  def doCommand(self):

    if 'name' not in self.args:
      return S_ERROR('NagiosProbesCommand: "name" not found in self.args')
    name = self.args['name']
    if name is None:
      return S_ERROR('NagiosProbesCommand: "name" should not be None')

    if 'flavor' not in self.args:
      return S_ERROR('NagiosProbesCommand: "flavor" not found in self.args')
    flavor = self.args['flavor']
    if flavor is None:
      return S_ERROR('NagiosProbesCommand: "flavor" should not be None')

    # ServiceURI is a quite misleading name.. it is a Resource in the RSS DB in fact.
    meta = {'columns': ['MetricStatus', 'SummaryData'],
            'count': True,
            'group': 'MetricStatus'}

    # FIXME: this command will not work, count and group options are not supported.
    res = self.rmClient.selectMonitoringTest(serviceURI=name, serviceFlavour=flavor,
                                             meta=meta)

    if not res['OK']:
      return res

    res = dict([(r[0], r[1:]) for r in res['Value']])

    return S_OK(res)
