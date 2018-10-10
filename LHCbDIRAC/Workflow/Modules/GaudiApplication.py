""" Gaudi Application module - main module: creates the environment,
    executes gaudirun with the right options

    This is the module used for each and every job of productions. It can also be used by users.
"""

__RCSID__ = "$Id$"

import re
import os
import subprocess
import shlex

from DIRAC import S_OK, S_ERROR, gLogger
from DIRAC.Core.Utilities import DErrno

from LHCbDIRAC.Core.Utilities.ProductionOptions import getDataOptions, getModuleOptions
from LHCbDIRAC.Core.Utilities.RunApplication import RunApplication, LbRunError, LHCbApplicationError, LHCbDIRACError
from LHCbDIRAC.Workflow.Modules.ModuleBase import ModuleBase


class GaudiApplication(ModuleBase):
  """ GaudiApplication class
  """

  def __init__(self, bkClient=None, dm=None):
    """ Usual init for LHCb workflow modules
    """

    self.log = gLogger.getSubLogger("GaudiApplication")
    super(GaudiApplication, self).__init__(self.log, bkClientIn=bkClient, dm=dm)

    self.systemConfig = ''
    self.stdError = ''
    self.runTimeProjectName = ''
    self.runTimeProjectVersion = ''
    self.inputDataType = 'MDF'
    self.stepInputData = []  # to be resolved
    self.poolXMLCatName = 'pool_xml_catalog.xml'
    self.optionsFile = ''
    self.optionsLine = ''
    self.extraOptionsLine = ''
    self.extraPackages = ''
    self.jobType = ''
    self.multicoreJob = 'True'
    self.multicoreStep = 'N'

  def _resolveInputVariables(self):
    """ Resolve all input variables for the module here.
    """

    super(GaudiApplication, self)._resolveInputVariables()
    super(GaudiApplication, self)._resolveInputStep()

  def execute(self, production_id=None, prod_job_id=None, wms_job_id=None,
              workflowStatus=None, stepStatus=None,
              wf_commons=None, step_commons=None,
              step_id=None, step_number=None):
    """ The main execution method of GaudiApplication. It runs a gaudirun app using RunApplication module.
        This is the module used for each and every job of productions. It can also be used by users.
    """

    try:
      super(GaudiApplication, self).execute(__RCSID__, production_id, prod_job_id, wms_job_id,
                                            workflowStatus, stepStatus,
                                            wf_commons, step_commons, step_number, step_id)

      if not self._checkWFAndStepStatus():
        return S_OK()

      self._resolveInputVariables()

      self.log.info("Executing application %s %s for CMT configuration %s" % (self.applicationName,
                                                                              self.applicationVersion,
                                                                              self.systemConfig))
      if self.jobType.lower() == 'merge' or 'BOINC' in self.siteName:
        self._disableWatchdogCPUCheck()

      # Resolve options files
      commandOptions = []
      if self.optionsFile and self.optionsFile != "None":
        for fileopt in self.optionsFile.split(';'):
          if os.path.exists('%s/%s' % (os.getcwd(), os.path.basename(fileopt))):
            commandOptions.append(fileopt)
          # Otherwise take the one from the application options directory
          elif re.search(r'\$', fileopt):
            self.log.info('Found options file containing environment variable: %s' % fileopt)
            commandOptions.append(fileopt)
          else:
            self.log.error(
                'Cannot process options: "%s" not found via environment variable or in local directory' %
                (fileopt))

      self.log.info('Final options files: %s' % (', '.join(commandOptions)))

      runNumberGauss = 0
      firstEventNumberGauss = 0
      if self.applicationName.lower() == "gauss" and self.production_id and self.prod_job_id:
        if self.jobType.lower() == 'user':
          eventsMax = self.numberOfEvents
        else:
          # maintaining backward compatibility
          eventsMax = self.maxNumberOfEvents if self.maxNumberOfEvents else self.numberOfEvents
        runNumberGauss = int(self.production_id) * 100 + int(self.prod_job_id)
        firstEventNumberGauss = eventsMax * (int(self.prod_job_id) - 1) + 1

      if self.optionsLine or self.jobType.lower() == 'user':
        self.log.debug("Won't get any step outputs (USER job)")
        stepOutputs = []
        stepOutputTypes = []
        histogram = False
      else:
        self.log.debug("Getting the step outputs")
        stepOutputs, stepOutputTypes, histogram = self._determineOutputs()
        self.log.debug(
            "stepOutputs, stepOutputTypes, histogram  ==>  %s, %s, %s" %
            (stepOutputs, stepOutputTypes, histogram))

      prodConfFileName = ''
      if self.optionsLine or self.jobType.lower() == 'user':
        # Prepare standard project run time options
        generatedOpts = 'gaudi_extra_options.py'
        if os.path.exists(generatedOpts):
          os.remove(generatedOpts)
        inputDataOpts = getDataOptions(self.applicationName,
                                       self.stepInputData,
                                       self.inputDataType,
                                       self.poolXMLCatName)['Value']  # always OK
        projectOpts = getModuleOptions(self.applicationName,
                                       self.numberOfEvents,
                                       inputDataOpts,
                                       self.optionsLine,
                                       runNumberGauss,
                                       firstEventNumberGauss,
                                       self.jobType)['Value']  # always OK
        self.log.info('Extra options generated for %s %s step:' % (self.applicationName, self.applicationVersion))
        print projectOpts  # Always useful to see in the logs (don't use gLogger as we often want to cut n' paste)
        with open(generatedOpts, 'w') as options:
          options.write(projectOpts)
        commandOptions.append(generatedOpts)

      else:
        prodConfFileName = self.createProdConfFile(stepOutputTypes, histogram, runNumberGauss, firstEventNumberGauss)

      # How to run the application
      ra = RunApplication()
      # lb-run stuff
      ra.applicationName = self.applicationName
      ra.applicationVersion = self.applicationVersion
      ra.systemConfig = self.systemConfig
      ra.extraPackages = self.extraPackages
      ra.runTimeProject = self.runTimeProjectName
      ra.runTimeProjectVersion = self.runTimeProjectVersion
      # actual stuff to run
      ra.command = self.executable
      ra.extraOptionsLine = self.extraOptionsLine
      ra.commandOptions = commandOptions
      if self.multicoreStep.upper() == 'Y':
        ra.multicore = self.multicoreJob
      ra.prodConfFileName = prodConfFileName
      if self.applicationLog:
        ra.applicationLog = self.applicationLog
      ra.stdError = self.stdError

      # Now really running
      try:
        self.setApplicationStatus('%s step %s' % (self.applicationName, self.step_number))
        ra.run()  # This would trigger an exception in case of failure, or application status != 0
      except LHCbApplicationError as appError:
        # Running gdb in case of core dump
        if 'core' in [fileProduced.split('.')[0] for fileProduced in os.listdir('.')]:
          # getting the environment where the application executed
          app = ra.applicationName + '/' + ra.applicationVersion
          envCommand = ra.lbrunCommand.split(app)[0] + ' --py -A ' + app

          # FIXME: The following may raise CalledProcessError if the application is not lb-run native.
          # The exception will be caught at the end of the module.
          # We enter here iff the application fails, and there's a core dump, so it's ~OKish.
          # Nevertheless, it would be better if it was handled correctly.
          lhcbApplicationEnv = eval(subprocess.check_output(shlex.split(envCommand)))

          # now running the GDB command
          gdbCommand = "gdb python core.* >> %s_Step%s_coredump.log" % (self.applicationName, self.step_number)
          rg = RunApplication()
          rg._runApp(gdbCommand, lhcbApplicationEnv)
        raise appError

      self.log.info("Going to manage %s output" % self.applicationName)
      self._manageAppOutput(stepOutputs)

      # Still have to set the application status e.g. user job case.
      self.setApplicationStatus('%s %s Successful' % (self.applicationName, self.applicationVersion))

      return S_OK("%s %s Successful" % (self.applicationName, self.applicationVersion))

    except LbRunError as lbre:  # This is the case for lb-run/environment errors
      self.setApplicationStatus(repr(lbre))
      return S_ERROR(DErrno.EWMSRESC, str(lbre))
    except LHCbApplicationError as lbae:  # This is the case for real application errors
      self.setApplicationStatus(repr(lbae))
      return S_ERROR(str(lbae))
    except LHCbDIRACError as lbde:  # This is the case for LHCbDIRAC errors (e.g. subProcess call failed)
      self.setApplicationStatus(repr(lbde))
      return S_ERROR(str(lbde))
    except Exception as exc:  # pylint:disable=broad-except
      self.log.exception("Failure in GaudiApplication execute module", lException=exc, lExcInfo=True)
      self.setApplicationStatus("Error in GaudiApplication module")
      return S_ERROR(str(exc))
    finally:
      super(GaudiApplication, self).finalize(__RCSID__)
