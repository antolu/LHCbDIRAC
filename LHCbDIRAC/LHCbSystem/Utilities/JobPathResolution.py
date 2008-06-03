########################################################################
# $Id: JobPathResolution.py,v 1.1 2008/06/03 12:48:29 paterson Exp $
# File :   JobPathResolution.py
# Author : Stuart Paterson
########################################################################

""" The job path resolution module is a VO-specific plugin that
    allows to define VO job policy in a simple way.  This allows the
    inclusion of LHCb specific WMS optimizers without compromising the
    generic nature of DIRAC.

    The arguments dictionary from the JobPathAgent includes the ClassAd
    job description and therefore decisions are made based on the existence
    of JDL parameters.
"""

__RCSID__ = "$Id: JobPathResolution.py,v 1.1 2008/06/03 12:48:29 paterson Exp $"

from DIRAC.Core.Utilities.ClassAd.ClassAdLight             import ClassAd
from DIRAC                                                 import S_OK, S_ERROR, gConfig, gLogger

import string,re

COMPONENT_NAME='LHCbJobPathResolution'

class JobPathResolution:

  #############################################################################
  def __init__(self,argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)

  #############################################################################
  def execute(self):
    """Given the arguments from the JobPathAgent, this function resolves job optimizer
       paths according to LHCb VO policy.
    """
    lhcbPath = ''

    if not self.arguments.has_key('ConfigPath'):
      self.log.warn('No CS ConfigPath defined')
      return S_ERROR('JobPathResoulution Failure')

    self.log.verbose('Attempting to resolve job path for LHCb')
    job = self.arguments['JobID']
    classadJob = self.arguments['ClassAd']
    section = self.arguments['ConfigPath']

    ancestorDepth = classadJob.get_expression('AncestorDepth').replace('"','').replace('Unknown','')
    if ancestorDepth:
      self.log.info('Job %s has specified ancestor depth' % (job))
      ancestors = gConfig.getValue(section+'/AncestorFiles','AncestorFiles')
      lhcbPath += ancestors+','

    inputDataType = classadJob.get_expression('InputDataType').replace('"','').replace('Unknown','')
    if inputDataType=='ETC':
      self.log.info('Job %s has ETC requirement' % (job))
      eventTagCollection = gConfig.getValue(section+'/ETC','ETC')
      lhcbPath += eventTagCollection+','

    if classadJob.lookupAttribute('CondDBTags'):
      condDB = gConfig.getValue(section+'/CondDB','CondDB')
      lhcbPath += condDB+','

    if not lhcbPath:
      self.log.info('No LHCb specific optimizers to be added')

    return S_OK(lhcbPath)

  #EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#
