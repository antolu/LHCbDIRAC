########################################################################
# $Id: JobFinalization.py,v 1.1 2008/02/08 09:04:41 joel Exp $
########################################################################
""" Book Keeping Report Class """

__RCSID__ = "$Id: JobFinalization.py,v 1.1 2008/02/08 09:04:41 joel Exp $"


from DIRAC import                                        *

import os, time, re

class JobFinalization(object):

  def __init__(self):
    self.STEP_ID = None
    self.CONFIG_NAME = None
    self.RUN_NUMBER = None
    self.FIRST_EVENT_NUMBER = None
    self.NUMBER_OF_EVENTS = None
    self.NUMBER_OF_EVENTS_OUTPUT = None
    self.log = gLogger.getSubLogger("JobFinalization")
    self.nb_events_input = None
    pass

  def execute(self):

    res = shellCall(0,'ls -al')
    self.log.info("final listing : %s" % (str(res)))
