""" GangaDataFile is a utility to create a Data file, to be used by ganga.

    Given input files, it will create something like:

    from Gaudi.Configuration import *
    from GaudiConf import IOHelper
    IOHelper("ROOT").inputFiles([
        "LFN:foo",
        "LFN:bar"
    ], clear=True)

    FileCatalog().Catalogs = ["xmlcatalog_file:pool_xml_catalog.xml"]
"""

__RCSID__ = "$Id$"

import os

from DIRAC import gLogger

from LHCbDIRAC.BookkeepingSystem.Client.LHCB_BKKDBClient import LHCB_BKKDBClient


class GangaDataFile(object):
  """ Creates ganga data file
  """

  def __init__(self, fileName='data.py', xmlcatalog_file='pool_xml_catalog.xml', log=None):
    """ initialize
    """
    if not log:
      self.log = gLogger.getSubLogger('GangaDataFile')
    else:
      self.log = log

    self.fileName = fileName
    self.xmlcatalog_file = xmlcatalog_file

    try:
      os.remove(self.fileName)
    except OSError:
      pass

    self.log.info('Creating Ganga data file %s from scratch' % self.fileName)

  ################################################################################

  def generateDataFile(self, lfns, persistency=None,
                       TSDefaultStr=None,
                       TSLookupMap=None):
    """ generate the data file
    """
    if isinstance(lfns, basestring):
      lfns = [lfns]
    elif not isinstance(lfns, list):
      self.log.error('Was expecting a list')
      raise TypeError('Expected List')
    if not len(lfns):
      self.log.warn('No file generated: was expecting a non-empty list')
      raise ValueError('list empty')

    try:
      persistency = persistency.upper()
    except AttributeError:
      pass

    # Create a fake LFN->PFN dictionary to give the persistency
    if persistency:
      fakePfns = dict.fromkeys(lfns, {'pfntype': persistency})
    else:
      fakePfns = None
    script = LHCB_BKKDBClient(welcome=False).writeJobOptions(lfns,
                                                             optionsFile=self.fileName,
                                                             catalog=self.xmlcatalog_file,
                                                             savePfn=fakePfns)
    self.log.info('Created Ganga data file %s' % self.fileName)

    return script
