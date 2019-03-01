
""" The job path resolution module is a VO-specific plugin that
    allows to define VO job policy in a simple way.  This allows the
    inclusion of LHCb specific WMS optimizers without compromising the
    generic nature of DIRAC.

    The arguments dictionary from the JobPathAgent includes the ClassAd
    job description and therefore decisions are made based on the existence
    of JDL parameters.
"""

from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.WorkloadManagementSystem.DB.JobDB import JobDB

COMPONENT_NAME = 'LHCbJobPathResolution'


class JobPathResolution:
  """ Main class for JobPathResolution """

  def __init__(self, argumentsDict):
    """ Standard constructor
    """
    self.arguments = argumentsDict
    self.name = COMPONENT_NAME
    self.log = gLogger.getSubLogger(self.name)

  def execute(self):
    """Given the arguments from the JobPathAgent, this function resolves job optimizer
       paths according to LHCb VO policy.
    """

    if 'ConfigPath' not in self.arguments:
      self.log.warn('No CS ConfigPath defined')
      return S_ERROR('JobPathResoulution Failure')

    self.log.verbose('Attempting to resolve job path for LHCb')
    return self.__jobStatePath(self.arguments['JobID'],
                               self.arguments['ConfigPath'],
                               self.arguments['JobState'])

  def __jobStatePath(self, jid, section, jobState):
    path = []
    result = jobState.getManifest()
    if not result['OK']:
      return result
    jobManifest = result['Value']

    ancestorDepth = jobManifest.getOption('AncestorDepth', '').replace('Unknown', '')
    if ancestorDepth:
      self.log.info('Job %s has specified ancestor depth' % (jid))
      ancestors = gConfig.getValue('%s/AncestorFiles' % section, 'AncestorFiles')
      path.append(ancestors)

    inputData = jobManifest.getOption("InputData", '').replace('Unknown', '')
    if inputData:
      if not jobManifest.getOption('DisableDataScheduling', False):
        self.log.info('Job %s has input data requirement' % (jid))
        path.append('InputData')
      else:
        self.log.info('Job %s has input data requirement but scheduling via input data is disabled' % (jid))
        result = JobDB().setInputData(jid, [])
        if not result['OK']:
          self.log.error(result)
          return S_ERROR('Could not reset input data to null')

    if not path:
      self.log.info('No LHCb specific optimizers to be added')

    return S_OK(path)
