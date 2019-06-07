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
""" LHCbDIRAC.ResourceStatusSystem.Client.ResourceManagementClient

   ResourceManagementClient.__bases__:
     DIRAC.ResourceStatusSystem.Client.ResourceManagementClient.ResourceManagementClient

"""

__RCSID__ = "$Id$"

# pylint: disable=unused-argument,too-many-arguments

from DIRAC import S_ERROR, S_OK
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient as DIRACRMClient


class ResourceManagementClient(DIRACRMClient):
  """
  Extension for the DIRAC version of the ResourceManagementClient.

  The :class:`ResourceManagementClient` class extends the client on DIRAC.

  It has the 'direct-db-access' functions, the ones of the type:
   - insert
   - update
   - get
   - delete

  that return parts of the RSSConfiguration stored on the CS, and used everywhere
  on the RSS module. Finally, and probably more interesting, it exposes a set
  of functions, badly called 'boosters'. They are 'home made' functions using the
  basic database functions that are interesting enough to be exposed.

  The client will ALWAYS try to connect to the DB, and in case of failure, to the
  XML-RPC server ( namely :class:`ResourceManagementDB` and
  :class:`ResourceManagementHancler` ).

  You can use this client on this way

   >>> from DIRAC.ResourceManagementSystem.Client.ResourceManagementClient import ResourceManagementClient
   >>> rsClient = ResourceManagementClient()

  All functions calling methods exposed on the database or on the booster are
  making use of some syntactic sugar, in this case a decorator that simplifies
  the client considerably.
  """

  def selectMonitoringTest(self, metricName=None, serviceURI=None,
                           siteName=None, serviceFlavour=None,
                           metricStatus=None, summaryData=None,
                           timestamp=None, lastCheckTime=None):
    """
    Gets from MonitoringTest all rows that match the parameters given.

    :Parameters:
      **metricName** - `[, string, list]`
        name of the metric
      **serviceURI** - `[, string, list]`
        URI of the service
      **siteName** - `[, string, list]`
        name of the site
      **serviceFlavour** - `[, string, list]`
        type of service
      **metricStatus** - `[, string, list]`
        metric's status
      **summaryData** - `[, string, list]`
        result of the monitoring test
      **timestamp** - `[, datetime, list]`
        timestamp of the test
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().select('MonitoringTest', self._prepare(locals()))

  def deleteMonitoringTest(self, metricName=None, serviceURI=None,
                           siteName=None, serviceFlavour=None,
                           metricStatus=None, summaryData=None,
                           timestamp=None, lastCheckTime=None):
    """
    Deletes from MonitoringTest all rows that match the parameters given.

    :Parameters:
      **metricName** - `[, string, list]`
        name of the metric
      **serviceURI** - `[, string, list]`
        URI of the service
      **siteName** - `[, string, list]`
        name of the site
      **serviceFlavour** - `[, string, list]`
        type of service
      **metricStatus** - `[, string, list]`
        metric's status
      **summaryData** - `[, string, list]`
        result of the monitoring test
      **timestamp** - `[, datetime, list]`
        timestamp of the test
      **lastCheckTime** - `[, datetime, list]`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().delete('MonitoringTest', self._prepare(locals()))

  def addOrModifyMonitoringTest(self, metricName, serviceURI, siteName,
                                serviceFlavour, metricStatus, summaryData,
                                timestamp, lastCheckTime):
    """
    Using `metricName` and `serviceURI` to query the database, decides whether
    to insert or update the table.

    :Parameters:
      **metricName** - `string`
        name of the metric
      **serviceURI** - `string`
        URI of the service
      **siteName** - `string`
        name of the site
      **serviceFlavour** - `string`
        type of service
      **metricStatus** - `string`
        metric's status
      **summaryData** - `string`
        result of the monitoring test
      **timestamp** - `datetime`
        timestamp of the test
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().addOrModify('MonitoringTest', self._prepare(locals()))

  ##############################################################################
  # JOB ACCOUNTING CACHE METHODS

  def selectJobAccountingCache(self, name=None, checking=None, completed=None,
                               done=None, failed=None, killed=None,
                               matched=None, running=None, stalled=None,
                               lastCheckTime=None):
    """
    Selects from JobAccountingCach all rows that match the parameters given.

    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element
      **checking** - [, `float`, `list` ]
        number of checking jobs
      **completed** - [, `float`, `list` ]
        number of completed jobs
      **done** - [, `float`, `list` ]
        number of done jobs
      **failed** - [, `float`, `list` ]
        number of failed jobs
      **killed** - [, `float`, `list` ]
        number of killed jobs
      **matched** - [, `float`, `list` ]
        number of matched jobs
      **running** - [, `float`, `list` ]
        number of running jobs
      **stalled** - [, `float`, `list` ]
        number of stalled jobs
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().select('JobAccountingCache', self._prepare(locals()))

  def deleteJobAccountingCache(self, name=None, checking=None, completed=None,
                               done=None, failed=None, killed=None,
                               matched=None, running=None, stalled=None,
                               lastCheckTime=None):
    """
    Deletes from JobAccountingCach all rows that match the parameters given.

    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element
      **checking** - [, `float`, `list` ]
        number of checking jobs
      **completed** - [, `float`, `list` ]
        number of completed jobs
      **done** - [, `float`, `list` ]
        number of done jobs
      **failed** - [, `float`, `list` ]
        number of failed jobs
      **killed** - [, `float`, `list` ]
        number of killed jobs
      **matched** - [, `float`, `list` ]
        number of matched jobs
      **running** - [, `float`, `list` ]
        number of running jobs
      **stalled** - [, `float`, `list` ]
        number of stalled jobs
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """
    return self._getRPC().delete('JobAccountingCache', self._prepare(locals()))

  def addOrModifyJobAccountingCache(self, name=None, checking=None, completed=None,
                                    done=None, failed=None, killed=None,
                                    matched=None, running=None, stalled=None,
                                    lastCheckTime=None):
    """
    Using `name` to query the database, decides whether to insert or update the t
    table.

    :Parameters:
      **name** - `string`
        name of the element
      **checking** - `float`
        number of checking jobs
      **completed** - `float`
        number of completed jobs
      **done** - `float`
        number of done jobs
      **failed** - `float`
        number of failed jobs
      **killed** - `float`
        number of killed jobs
      **matched** - `float`
        number of matched jobs
      **running** - `float`
        number of running jobs
      **stalled** - `float`
        number of stalled jobs
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().addOrModify('JobAccountingCache', self._prepare(locals()))

  ##############################################################################
  # PILOT ACCOUNTING CACHE METHODS

  def selectPilotAccountingCache(self, name=None, aborted=None, deleted=None,
                                 done=None, failed=None, lastCheckTime=None,
                                 meta=None):
    """
    Selects from PilotAccountingCache all rows that match the parameters given.

    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element
      **aborted** - [, `float`, `list` ]
        number of aborted pilots
      **deleted** - [, `float`, `list` ]
        number of deleted pilots
      **done** - [, `float`, `list` ]
        number of done pilots
      **failed** - [, `float`, `list` ]
        number of failed pilots
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """
    return self._getRPC().select('PilotAccountingCache', self._prepare(locals()))

  def deletePilotAccountingCache(self, name=None, aborted=None, deleted=None,
                                 done=None, failed=None, lastCheckTime=None,
                                 meta=None):
    """
    Deletes from PilotAccountingCache all rows that match the parameters given.

    :Parameters:
      **name** - [, `string`, `list` ]
        name of the element
      **aborted** - [, `float`, `list` ]
        number of aborted pilots
      **deleted** - [, `float`, `list` ]
        number of deleted pilots
      **done** - [, `float`, `list` ]
        number of done pilots
      **failed** - [, `float`, `list` ]
        number of failed pilots
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """
    return self._getRPC().delete('PilotAccountingCache', self._prepare(locals()))

  def addOrModifyPilotAccountingCache(self, name=None, aborted=None, deleted=None,
                                      done=None, failed=None, lastCheckTime=None,
                                      meta=None):
    """
    Using `name` to query the database, decides whether to insert or update the t
    table.

    :Parameters:
      **name** - `string`
        name of the element
      **aborted** - `float`
        number of aborted pilots
      **deleted** - `float`
        number of deleted pilots
      **done** - `float`
        number of done pilots
      **failed** - `float`
        number of failed pilots
      **lastCheckTime** - `datetime`
        last time it was cheched

    :return: S_OK() || S_ERROR()
    """

    return self._getRPC().addOrModify('PilotAccountingCache', self._prepare(locals()))

  def getSEStorageSpace(self, seName):
    """ getSEStorageSpace

    Given a SE, returns a dictionary with the Total, Free and Guaranteed Space.
    This last one, is still unclear what represents ( so far, is equal to Total
    space, but that might change ).

    This is how this new method should be used, and also what it returns ( returns
    a dictionary, so there is no need to generate it - dict( zip ( ... ) ) ).

    >>> res = self.getSEStorageSpace( 'CERN-RAW' )
    >>> { 'Endpoint'      : 'httpg://srm-lhcb.cern.ch:8443/srm/managerv2',
          'LastCheckTime' : datetime.datetime(2013, 10, 24, 12, 2, 17),
          'Guaranteed'    : 465L,
          'Free'          : 60L,
          'Token'         : 'LHCb-Tape',
          'Total'         : 465L
         }

    """

    # FIXME:  The DB contains the SE name as token... This is dirty but mandatory
    res = self.selectSpaceTokenOccupancyCache(token=seName)
    if not res['OK']:
      return res
    if not res['Value']:
      return S_ERROR("Empty result")

    return S_OK(dict(zip(res['Columns'], res['Value'][0])))
