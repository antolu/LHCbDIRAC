""" UploadMC module is used to upload to ES the json files for MC statistics
"""

__RCSID__ = "$Id$"

import json

from DIRAC import S_OK, S_ERROR, gLogger
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase
from LHCbDIRAC.ProductionManagementSystem.Client.MCStatsClient import MCStatsClient


class UploadMC(ModuleBase):
  """ Upload to LogSE
  """

  def __init__(self):
    """Module initialization.
    """

    self.log = gLogger.getSubLogger("UploadMC")
    super(UploadMC, self).__init__(self.log)

    self.version = __RCSID__

  def _resolveInputVariables(self):
    """ standard method for resolving the input variables
    """

    super(UploadMC, self)._resolveInputVariables()

  def execute(self, production_id=None, prod_job_id=None, wms_job_id=None,
              workflowStatus=None, stepStatus=None,
              wf_commons=None, step_commons=None,
              step_number=None, step_id=None):
    """ Main executon method
    """
    try:

      super(UploadMC, self).execute(self.version, production_id, 
                                    prod_job_id, wms_job_id,
                                    workflowStatus, stepStatus,
                                    wf_commons, step_commons, 
                                    step_number, step_id)

      self._resolveInputVariables()

      if self.applicationName.lower() not in ('gauss', 'boole'):
        self.log.info('Not Gauss nor Boole, exiting')
        return S_OK()

      # looking for json files that are
      # 'self.jobID_Errors_self.applicationName_self.applicationVersion_self.step_number.json'
      jsonData = json.loads('%s_Errors_%s_%s_%s.json' % (self.jobID,
                                                         self.applicationName,
                                                         self.applicationVersion,
                                                         self.step_number))
      MCStatsClient.set('LogErr', jsonData)

    except Exception as e:  # pylint:disable=broad-except
      self.log.exception("Failure in UploadMC execute module", lException=e)
      return S_ERROR(str(e))

    finally:
      super(UploadMC, self).finalize(self.version)