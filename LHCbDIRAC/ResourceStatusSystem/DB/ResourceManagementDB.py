''' LHCbDIRAC.ResourceStatusSystem.DB.ResourceManagementDB

    ResourceManagementDB.__bases__:
      DIRAC.ResourceStatusSystem.DB.ResourceManagementDB.ResourceManagementDB

    Extension of ResourceManagementDB, adding the following tables:
    - MonitoringTest
    - JobAccountingCache
    - PilotAccountingCache
    - SLST1Service (obsolete)
    - SLSLogSE (obsolete)
'''

__RCSID__ = "$Id$"


import datetime

from sqlalchemy.dialects.mysql import INTEGER, TIMESTAMP, TINYINT, BIGINT
from sqlalchemy import Column, String, DateTime, Text, text, BLOB

from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import rmsBase, TABLESLIST


TABLESLIST = TABLESLIST + ['MonitoringTest',
                           'JobAccountingCache',
                           'PilotAccountingCache',
                           'SLST1Service',
                           'SLSLogSE']


class MonitoringTest(rmsBase):
  """ MonitoringTest table
  """

  __tablename__ = 'MonitoringTest'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  serviceuri = Column('ServiceURI', String(128), nullable=False, primary_key=True)
  metricname = Column('MetricName', String(128), nullable=False, primary_key=True)
  serviceflavour = Column('ServiceFlavour', String(64), nullable=False)
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)
  metricstatus = Column('MetricStatus', String(512), nullable=False)
  sitename = Column('SiteName', String(64), nullable=False)
  timestamp = Column('Timestamp', DateTime, nullable=False)
  summarydata = Column('SummaryData', BLOB, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the MonitoringTest object from a dictionary
    """

    utcnow = self.lastchecktime if self.lastchecktime else datetime.datetime.utcnow().replace(microsecond=0)

    self.serviceuri = dictionary.get('ServiceURI', self.serviceuri)
    self.metricname = dictionary.get('MetricName', self.metricname)
    self.serviceflavour = dictionary.get('ServiceFlavour', self.serviceflavour)
    self.lastchecktime = dictionary.get('LastCheckTime', self.lastchecktime)
    self.metricstatus = dictionary.get('MetricStatus', self.metricstatus)
    self.sitename = dictionary.get('SiteName', self.sitename)
    self.timestamp = dictionary.get('TimeStamp', utcnow)
    self.summarydata = dictionary.get('SummaryData', self.summarydata)

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.serviceuri, self.metricname, self.serviceflavour, self.lastchecktime, self.metricstatus,
            self.metricstatus, self.sitename, self.timestamp, self.summarydata]


class JobAccountingCache(rmsBase):
  """ JobAccountingCache table
  """

  __tablename__ = 'JobAccountingCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column('Name', String(64), nullable=False, primary_key=True)
  failed = Column('Failed', INTEGER, nullable=False, server_default='0')
  running = Column('Running', INTEGER, nullable=False, server_default='0')
  done = Column('Done', INTEGER, nullable=False, server_default='0')
  stalled = Column('Stalled', INTEGER, nullable=False, server_default='0')
  checking = Column('Checking', INTEGER, nullable=False, server_default='0')
  completed = Column('Completed', INTEGER, nullable=False, server_default='0')
  killed = Column('Killed', INTEGER, nullable=False, server_default='0')
  matched = Column('Matched', INTEGER, nullable=False, server_default='0')
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the JobAccountingCache object from a dictionary
    """

    utcnow = self.lastchecktime if self.lastchecktime else datetime.datetime.utcnow().replace(microsecond=0)

    self.name = dictionary.get('Name', self.name)
    self.failed = dictionary.get('Failed', self.failed)
    self.running = dictionary.get('Running', self.running)
    self.done = dictionary.get('Done', self.done)
    self.stalled = dictionary.get('Stalled', self.stalled)
    self.checking = dictionary.get('Checking', self.checking)
    self.completed = dictionary.get('Completed', self.completed)
    self.killed = dictionary.get('Killed', self.killed)
    self.matched = dictionary.get('Matched', self.matched)
    self.lastchecktime = dictionary.get('LastCheckTime', utcnow)

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.name, self.failed, self.running, self.done, self.stalled, self.checking,
            self.completed, self.killed, self.matched, self.lastchecktime]


class PilotAccountingCache(rmsBase):
  """ PilotAccountingCache table
  """

  __tablename__ = 'PilotAccountingCache'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column('Name', String(64), nullable=False, primary_key=True)
  failed = Column('Failed', INTEGER, nullable=False, server_default='0')
  deleted = Column('Deleted', INTEGER, nullable=False, server_default='0')
  done = Column('Done', INTEGER, nullable=False, server_default='0')
  aborted = Column('Aborted', INTEGER, nullable=False, server_default='0')
  lastchecktime = Column('LastCheckTime', DateTime, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the PilotAccountingCache object from a dictionary
    """

    utcnow = self.lastchecktime if self.lastchecktime else datetime.datetime.utcnow().replace(microsecond=0)

    self.name = dictionary.get('Name', self.name)
    self.failed = dictionary.get('Failed', self.failed)
    self.deleted = dictionary.get('Deleted', self.deleted)
    self.done = dictionary.get('Done', self.done)
    self.aborted = dictionary.get('Aborted', self.aborted)
    self.lastchecktime = dictionary.get('LastCheckTime', utcnow)

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.name, self.failed, self.deleted, self.done, self.aborted, self.lastchecktime]


# TABLES THAT WILL EVENTUALLY BE DELETED

class SLST1Service(rmsBase):
  """ SLST1Service table
  """

  __tablename__ = 'SLST1Service'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  site = Column('Site', String(64), nullable=False, primary_key=True)
  system = Column('System', String(32), nullable=False, primary_key=True)
  hostuptime = Column('HostUptime', INTEGER)
  version = Column('Version', String(32))
  serviceuptime = Column('ServiceUptime', INTEGER)
  timestamp = Column('TimeStamp', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  message = Column('Message', Text)
  availability = Column('Availability', TINYINT, nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the SLST1Service object from a dictionary
    """

    utcnow = self.lastchecktime if self.lastchecktime else datetime.datetime.utcnow().replace(microsecond=0)

    self.site = dictionary.get('Site', self.site)
    self.system = dictionary.get('System', self.system)
    self.hostuptime = dictionary.get('HostUpTime', self.hostuptime)
    self.version = dictionary.get('Version', self.version)
    self.serviceuptime = dictionary.get('ServiceUpTime', self.serviceuptime)
    self.timestamp = dictionary.get('TimeStamp', utcnow)
    self.message = dictionary.get('Message', self.message)
    self.availability = dictionary.get('Availability', self.availability)

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.site, self.system, self.hostuptime, self.version, self.serviceuptime,
            self.timestamp, self.message, self.availability]


class SLSLogSE(rmsBase):
  """ SLSLogSE table
  """

  __tablename__ = 'SLSLogSE'
  __table_args__ = {'mysql_engine': 'InnoDB',
                    'mysql_charset': 'utf8'}

  name = Column('Name', String(32), primary_key=True)
  availability = Column('Availability', TINYINT, nullable=False)
  timestamp = Column('TimeStamp', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP'))
  datapartitiontotal = Column('DataPartitionTotal', BIGINT)
  datapartitionused = Column('DataPartitionUsed', TINYINT)
  validityduration = Column('ValidityDuration', String(32), nullable=False)

  def fromDict(self, dictionary):
    """
    Fill the fields of the SLSLogSE object from a dictionary
    """

    self.name = dictionary.get('Name', self.name)
    self.timestamp = dictionary.get('TimeStamp', self.timestamp)
    self.availability = dictionary.get('Availability', self.availability)
    self.datapartitiontotal = dictionary.get('DataPartitionTotal', self.datapartitiontotal)
    self.datapartitionused = dictionary.get('DataPartitionUsed', self.datapartitionused)
    self.validityduration = dictionary.get('ValidityDuration', self.validityduration)

  def toList(self):
    """ Simply returns a list of column values
    """
    return [self.name, self.timestamp, self.availability,
            self.datapartitiontotal, self.datapartitionused, self.validityduration]
