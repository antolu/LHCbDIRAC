""" UploadMC module is used to upload to ES the json files for MC statistics
"""

__RCSID__ = "$Id$"

import os
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

      # looking for json files that are 'self.jobID_Errors_appName.json'
      for app in ['Gauss', 'Boole']:
        fn = '%s_Errors_%s.json' % (self.jobID, app)
        if os.path.exists(fn):
          with open(fn) as fd:
            try:
              jsonData = json.load(fd)
              self.log.verbose(jsonData)
              if self._enableModule():
                res = MCStatsClient().set('LogErr', 'json', str(jsonData))
                if not res['OK']:
                  self.log.error('%s not set, exiting without affecting workflow status' % jsonData, res['Message'])
              else:
                # At this point we can see exactly what the module would have uploaded
                self.log.info("Would have attempted to upload the following file %s" % fn)
            except BaseException as ve:
              self.log.verbose("Exception loading the JSON file: content of %s follows" % fn)
              print fd.read()
              raise ve
        else:
          self.log.info("JSON file %s not found" % fn)

      return S_OK()

    except Exception as e:
      self.log.exception("Failure in UploadMC execute module", lException=e)
      return S_ERROR(repr(e))

    finally:
      super(UploadMC, self).finalize(self.version)
