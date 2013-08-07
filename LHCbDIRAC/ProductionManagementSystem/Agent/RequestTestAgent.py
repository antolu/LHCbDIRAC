''' Production request test agent monitors testing database
    and perform the test locally.
'''

import cPickle
import os
import re
import tempfile

import DIRAC

from DIRAC                           import S_OK, S_ERROR, gLogger
from DIRAC.Core.Base.AgentModule     import AgentModule
from DIRAC.Core.DISET.RPCClient      import RPCClient
from DIRAC.Core.Utilities.Subprocess import shellCall

__RCSID__ = "$Id$"
# AZ: Copy/paste from the controller... should be in a "library"

class PrTpl(object):
  """ Production Template engine
  AZ: I know it is not the best :)
  It undestand: {{<sep><ParName>[#<Label>]}}
  Where:
    <sep> - not alpha character to be inserted
            before parameter value in case it
            is not empty
    <ParName> - parameter name (must begin
            with alpha character) to substitute
    <Label> - if '#' is found after '{{', everything after
            it till '}}' is Label for the parameter
            (default to ParName) In case you use one parameter
            several times, you can specify the label only once.
  """
  __par_re  = "\{\{(\W)?([^\}#]+)#?([^\}#]*)#?([^\}#]*)\}\}"
  __par_sub = "\{\{(\W|)?%s[^\}]*\}\}"
  def __init__( self, tpl_xml ):
    self.tpl = tpl_xml
    self.pdict = {}
    self.ddict = {}
    for x in re.findall(self.__par_re, self.tpl):
      if not x[1] in self.ddict or x[3]:
        dvalue = x[3]
        if not dvalue:
          dvalue = ''
        self.ddict[x[1]] = dvalue
      if x[1] in self.pdict and not x[2]:
        continue
      label = x[2]
      if not label:
        label = x[1]
      self.pdict[x[1]] = label

  def getParams(self):
    """ Return the dictionary with parameters (value is label) """
    return self.pdict

  def getDefaults(self):
    """ Return the dictionary with parameters defaults """
    return self.ddict

  def apply(self, pdict):
    """ Return template with substituted values from pdict """
    result = self.tpl
    for p in self.pdict:
      value = str(pdict.get(p, ''))
      if value:
        value = "\\g<1>" + value
      result = re.sub(self.__par_sub % p, value, result)
    return result


AGENT_NAME = 'ProductionManagement/RequestTestAgent'

def runTest(script,data):
  """ Run the test """
  try:
    fTmp = tempfile.mkstemp()
    os.write(fTmp[0], data)
    os.close(fTmp[0])
    fs= tempfile.mkstemp()
    os.write(fs[0], script)
    os.close(fs[0])
  except Exception, msg:
    gLogger.error("In temporary files createion: "+str(msg))
    return S_ERROR(str(msg))
  setenv = "source /opt/dirac/bashrc"
  cmd = "python %s %s" % (fs[1], fTmp[1])
  try:
    res = shellCall(1800, [ "/bin/bash -c '%s;%s'" \
                                 % (setenv, cmd) ])
    if res['OK']:
      result = S_OK(str(res['Value'][1])+str(res['Value'][2]))
    else:
      gLogger.error(res['Message'])
      result = res
  except Exception, msg:
    gLogger.error("During execution: "+str(msg))
    result = S_ERROR("Failed to execute: %s" % str(msg))
  os.remove(fTmp[1])
  os.remove(fs[1])
  return result

class RequestTestAgent(AgentModule):

  def initialize(self):
    """Sets defaults"""
    self.pollingTime = self.am_getOption('PollingTime', 1200)
    self.setup       = self.am_getOption('Setup', '')
    return S_OK()

  def getTests2Run(self):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.getTests("Waiting")

  def testResult(self, iD, state, link):
    RPC   = RPCClient( "ProductionManagement/ProductionRequest", setup=self.setup )
    return RPC.setTestResult(iD, state, link)

  def execute(self):
    """The RequestTestAgent execution method.
    """
    gLogger.info('Request Test execute is started')

    result = self.getTests2Run()
    if result['OK']:
      for test in result['Value']:
        gLogger.info('Testing %s' % str(test['RequestID']))
        d = {}
        d.update(cPickle.loads(test['Input']))
        d.update(cPickle.loads(test['Params']))
        script = PrTpl(cPickle.loads(test['Script'])).apply(d)
        data = PrTpl(cPickle.loads(test['Template'])).apply(d)
        state = ''
        result = self.testResult(test['RequestID'], "Run", "")
        if not result['OK']:
          gLogger.error('Failed to indicate test run: %s' % result['Message'])
        result = runTest(script, data)
        if result['OK']:
          sout = result['Value'].split('\n')
          if len(sout) > 2 and len(sout[-2]) < 32:
            state = sout[-2]
            link = sout[-3]
        if state and len(state.split(' ')) == 1 and state.find(':') == -1:
          gLogger.info('Result: "%s" "%s"' % (state, link))
        else:
          gLogger.error('Bad script, trace: %s' % result['Value'])
          state = 'Failed'
          link = 'Bug in agent'
        result = self.testResult(test['RequestID'], state, link)
        if not result['OK']:
          gLogger.error('Failed to upload test result: %s' % result['Message'])
    else:
      gLogger.error('Request service: %s' % result['Message'])

    gLogger.info('Request Test is ended')
    return S_OK('Request Test is ended')
