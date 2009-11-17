########################################################################
# $HeadURL$
# File :   ProductionManagerCLI.py
# Author : Adria Casajus
########################################################################
__RCSID__   = "$Id$"
__VERSION__ = "$Revision: 1.18 $"

import cmd
import sys, os
import signal
import string
import time
from DIRAC import S_OK, S_ERROR, gConfig, gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
from DIRAC.TransformationSystem.Client.TransformationDBCLI   import TransformationDBCLI
#job submission
from LHCbDIRAC.LHCbSystem.Client.DiracProduction          import DiracProduction

def printDict(dictionary):
  """ Dictionary pretty printing
  """

  key_max = 0
  value_max = 0
  for key,value in dictionary.items():
    if len(key) > key_max:
      key_max = len(key)
    if len(str(value)) > value_max:
      value_max = len(str(value))
  for key,value in dictionary.items():
    print key.rjust(key_max),' : ',str(value).ljust(value_max)

class ProductionManagerCLI( TransformationDBCLI ):

  def __init__( self ):
    TransformationDBCLI.__init__( self )
    self.identSpace = 20
    self.server=RPCClient('ProductionManagement/ProductionManager')
    self.lfc = None

  #def stripDN(self, dn):
  #    df.rfind

################ WORKFLOW SECTION ####################################

  def do_uploadWorkflow(self, args):
    """ Upload Workflow in the repository

      Usage: uploadWorkflow <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, publishing will be refused.
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    tr_file = argss[0]

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.server.publishWorkflow(body, False)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file

  def do_updateWorkflow(self, args):
    """ Publish or Update Workflow in the repository

      Usage: updateWorkflow <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, it will be replaced.
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    tr_file = argss[0]

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.server.publishWorkflow(body, True)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file


  def do_getWorkflow(self, args):
    """ Read Workflow from the repository

      Usage: getWorkflow <WFName> <filename>
      <WFName> - the name of the workflow
      <filename> is a path to the file to write xml of the workflow
    """

    argss, length = self.check_params(args, 2)
    if not argss:
      return
    wf_name = argss[0]
    path = argss[1]

    result = self.server.getWorkflow(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    if not result['Value']:
      print "No Workflow %s in the repository" % wf_name
      return

    body = result['Value']
    fd = open( path, 'wb' )
    fd.write(body)
    fd.close()

  def do_deleteWorkflow(self, args):
    """ Delete Workflow from the the repository

      Usage: deleteWorkflow WorkflowName
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    wf_name = argss[0]
    result = self.server.deleteWorkflow(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_listWorkflow(self, args):
    """ List all Workflows in the repository

      Usage: listWorkflow [-d]

      -d    more detailed output
    """

    argss = args.split()
    detailed = False
    if len(args) > 0:
      if argss[0] == '-d':
        detailed = True

    result = self.server.getListWorkflows()
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
    else:
      if detailed:
        print "----------------------------------------------------------------------------------"
        print "|    Name    |   Parent   |         Time        |          DN          |   Group   | Short Description |   Long Description   |"
        print "----------------------------------------------------------------------------------"
        for wf in result['Value']:
          print "| %010s | %010s | %014s | %s | %s | %s | %s |" % (wf['WFName'],wf['WFParent'],wf['PublishingTime'],wf['AuthorDN'][wf['AuthorDN'].rfind('/CN=')+4:],
                                                         wf['AuthorGroup'],wf['Description'],wf['LongDescription'])
        print "----------------------------------------------------------------------------------"
      else:
        for wf in result['Value']:
          print "%s %s %s %s %s" % (wf['WFName'].ljust(25),
                                    wf['PublishingTime'].ljust(20),
                                    wf['AuthorDN'][wf['AuthorDN'].rfind('/CN=')+4:].ljust(20),
                                    wf['AuthorGroup'].ljust(12),
                                    wf['Description'].ljust(20))


  def do_getWorkflowInfo(self, args):
    """
    Read Workflow from the repository
      Usage: getWorkflowInfo <WFName>
      <WFName> - the name of the workflow
    """

    argss, length = self.check_params(args, 1)
    if not argss:
      return
    wf_name = argss[0]
    result = self.server.getWorkflowInfo(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
    printDict(result['Value'])

################ PRODUCTION SECTION ####################################

  def do_createProduction(self, args):
    """ Upload Production in to the transformation table

      Usage: createProduction <filename> <filemask> <groupsize>
      <filename> is a path to the file with the xml description of the workflow
      If transformation with this name already exists, publishing will be refused.
      <filemask> mask to match files going to be accepted by transformation
      <groupsize> how many files going to be grouped per job
      WARNING!!! if <filemask> and <groupsize> are absent, the system will create 'SIMULATION' type of transformation
    """
    tr_mask = ''
    tr_groupsize = 0
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    tr_file = argss[0]
    if length>1:
      tr_mask = argss[1]
    else:
      tr_mask = ''
    if length>2:
      tr_groupsize = int(argss[2])

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.server.publishProduction(body, tr_mask, tr_groupsize, False)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file

  def do_createDerivedProduction(self, args):
    """
    Upload Production in to the transformation table
      Usage: createDerivedProduction <ProductionNameOrID> <filename> <filemask> <groupsize>
      <ProductionNameOrID> ID or name production used as derived
      <filename> is a path to the file with the xml description of the workflow
      If transformation with this name already exists, publishing will be refused.
      <filemask> mask to match files going to be accepted by transformation
      <groupsize> how many files going to be grouped per job
      WARNING!!! if <filemask> and <groupsize> are absent, the system will create 'SIMULATION' type of transformation
    """
    tr_mask = ''
    tr_groupsize = 0
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    ProductionNameOrID = self.check_id_or_name(argss[0])
    tr_file = argss[1]
    if length>2:
      tr_mask = argss[2]
    else:
      tr_mask = ''
    if length>3:
      tr_groupsize = int(argss[3])

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.server.publishDerivedProduction(ProductionNameOrID, body, tr_mask, tr_groupsize, False)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file

#  def do_updatePR(self, args):
#    """
#    Replace Production in the transformation table even old one with this name exists
#      Usage: updatePR <filename> <filemask> <groupsize>
#      <filename> is a path to the file with the xml description of the workflow
#      If transformation with this name already exists, publishing will be refused.
#      <filemask> mask to match files going to be accepted by transformation
#      <groupsize> how many files going to be grouped per job
#      WARNING!!! if <filemask> and <groupsize> are absent, the system will create 'SIMULATION' type of transformation
#    """
#    tr_mask = ''
#    tr_groupsize = 0
#    argss, length = self.check_params(args, 1)
#    if not argss:
#      return
#    tr_file = argss[0]
#    if length>1:
#      tr_mask = argss[1]
#    else:
#      tr_mask = ''
#    if length>2:
#      tr_groupsize = int(argss[2])
#
#    if os.path.exists(tr_file):
#      fd = file( tr_file )
#      body = fd.read()
#      fd.close()
#      result = self.server.publishProduction(body, tr_mask, tr_groupsize, True)
#      if not result['OK']:
#        print "Error during command execution: %s" % result['Message']
#    else:
#      print "File %s does not exists" % tr_file

  def do_getProductionBody(self, args):
    """
    Read Workflow from the repository
      Usage: getProductionBody <ProductionNameOrID> <filename>
      <ProductionNameOrID> - Production Name or ID
      <filename> is a path to the file to write xml
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    path = argss[1]

    result = self.server.getProductionBody(prodID)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    if not result['Value']:
      print "No body of production %s was found" % prodID
      return

    body = result['Value']
    fd = open( path, 'wb' )
    fd.write(body)
    fd.close()

  def do_setProductionBody(self, args):
    """
    Upload new body for the production
      Usage: setProductionBody <ProductionNameOrID> <filename>
      <ProductionNameOrID> - Production Name or ID
      <filename> is a path to the file with body
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    path = argss[1]

    if os.path.exists(path):
      fd = file( path )
      body = fd.read()
      fd.close()
      result = self.server.setProductionBody(prodID, body)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % path

  def do_extendProduction(self, args):
    """ Extend Simulation type Production by nJobs numer of jobs
      Usage: extendProduction <ProductionNameOrID> nJobs
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return

    prodID = self.check_id_or_name(argss[0])
    nJobs = int(argss[1])
    result = self.server.extendProduction(prodID,nJobs)
    print result

  def do_deleteProduction(self, args):
    """
    Delete Production from the the repository
      Usage: deleteProduction <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    print self.server.deleteProduction(prodID)

  def do_listProduction(self, args):
    """ List all Productions in the repository

      Usage: listProduction [-d]
    """

    argss = args.split()
    detailed = False
    if len(args) > 0:
      if argss[0] == '-d':
        detailed = True

    result = self.server.getAllProductions()
    if not result['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      if detailed:
        print "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
        print "| ID |    Name    |  Parent  |   Description   |   LongDescription  | CreationDate |   Author   | AuthorGroup |  Type    | Plugin | AgentType | Status | FileMask | GroupSize | InheritedFrom |"
        print "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
        for pr in result['Value']:
          print "| %06s | %010s | %08s | %010s | %08s | %08s | %08s | %014s | %s | %s | %s | %s | %s | %s | %s |" % (pr["TransID"],
                 pr['Name'], pr['Parent'], pr['Description'], pr['LongDescription'], pr['CreationDate'],
                 pr['AuthorDN'], pr['AuthorGroup'], pr['Type'], pr['Plugin'], pr['AgentType'], pr['Status'], pr['FileMask'], pr['GroupSize'], pr['InheritedFrom'])
        print "-----------------------------------------------------------------------------------------------------------------------------------------"
      else:
        for pr in result['Value']:
          print "%s %s %s %s %s" % (str(pr['TransID']).ljust(8),
                                    pr['Status'].ljust(12),
                                    str(pr['CreationDate']).ljust(20),
                                    pr['AuthorDN'][pr['AuthorDN'].rfind('/CN=')+4:].ljust(20),
                                    pr['Description'].ljust(32))

  def do_getProductionInfo(self, args):
    """Returns information about production

      Usage: getProductionInfo <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    result = self.server.getProductionInfo(prodID)

    if result['OK']:

      if result['Value'].has_key('Message'):
        if result['Value']['Message'].find('not found') != -1:
          print "Transformation %s not found" % prodID
          return

      dict = result['Value']['Value']
      del result['Value']['Value']
      del result['OK']
      del result['rpcStub']
      dict.update(result['Value'])
      del result['Value']
      print "\nGeneral information:"
      printDict(dict)

      if dict['BkQueryID'] != 0:
        result = self.server.getBookkeepingQuery(dict['BkQueryID'])
        if result['OK']:
          print "\nInput Data Bookkeeping Query:"
          printDict(result['Value'])

    result = self.server.getJobWmsStats(prodID)
    if result['OK']:
      print "\nJob statistics:"
      #printDict(result['Value'])
    result = self.server.getJobStats(prodID)
    if result['OK']:
      printDict(result['Value'])
    result = self.server.getTransformationStats(prodID)
    if result['OK']:
      print "\nTransformation files statistics:"
      printDict(result['Value'])


  def do_setProductionStatus(self, args):
    """ Set status of the production

    Usage: setProductionStatus ProdName Status
      New - newly created, equivalent to STOPED
      Active - can submit
      Flush - final stage, ignoring GroupSize
      Stopped - stopped by manager
      Done - job limits reached, extension is possible
      Error - Production with error, equivalent to STOPPED
      Terminated - stopped, extension impossible
      Case of the letters will be ignored
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    status = argss[1].lower().capitalize()
    self.server.setTransformationStatus(prodID, status)

  def do_setProductionPlugin(self, args):
    """ Set status of the production

    Usage: setProductionStatus ProdName Status
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    self.server.setTransformationPlugin(prodID, plugin)

  def do_setProductionParameter(self, args):
    """ Set production parameter

    Usage: setProductionParameter <ProductionNameOrID> <param_name> <param_value>
    """
    argss, length = self.check_params(args, 3)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    pname = argss[1]
    pvalue = argss[2]
    result = self.server.addTransformationParameter(prodID,pname,pvalue)
    if not result['OK']:
      print "Command failed with message:",result['Message']

  def do_getProductionLog(self, args):
    """ Get Log of the given ProductionID

    Usage: getProductionLog <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    result = self.server.getTransformationLogging(prodID)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    for mess in result['Value']:
      print mess['MessageDate'],": ", mess['Message'], " by ", mess['AuthorDN']

  def do_setSubmissionType(self, args):
    """ Set status of the production

    Usage: setSubmissionType <ProdNameOrID> <SubmissionStatus>
      Manual - submission by production manager only
      Automatic - submission by ProductionJobAgent
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    status = argss[1].lower().capitalize()
    self.server.setTransformationAgentType(prodID, status)

  def do_setProductionMask(self, args):
    """ Overrides transformation mask for the production

    Usage: setProductionMask <ProdNameOrID> <Mask>
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    mask = argss[1]
    self.server.setTransformationMask(prodID, mask)

  def do_updateProduction(self, args):
    """ Updates the files for a given production

    Usage: updateProduction <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    print self.server.updateTransformation(prodID)

  def do_setProductionQuery(self, args):
    """ Overrides transformation mask for the production

    Usage: setProductionQuery <ProdNameOrID> <queryID>
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    queryID = int(argss[1])
    result = self.server.setTransformationQuery(prodID, queryID)
    if not result['OK']:
      print "Operation failed: ",result['Message']

  def do_createBkQuery(self,args):
    """ Create a new Bookkeeping Query to be used in production definitions.

        Usage: createBkQuery [prodID]

        - prodID - production ID to be used for initial query definition
    """

    fields = ['SimulationConditions',
              'DataTakingConditions',
              'ProcessingPass',
              'FileType',
              'EventType',
              'ConfigName',
              'ConfigVersion',
              'ProductionID',
              'DataQualityFlag']

    resultQuery = {'SimulationConditions':'All',
                'DataTakingConditions':'All',
                'ProcessingPass':'No default!',
                'FileType':'DST',
                'EventType':90000000,
                'ConfigName':'All',
                'ConfigVersion':'All',
                'ProductionID':0,
                'DataQualityFlag':'OK'}

    argss, length = self.check_params(args, 0)
    if length:
      prodID = int(argss[0])
      result = self.server.getBookkeepingQueryForTransformation(prodID)
      if not result['OK']:
        print "Failed to get initial query"
        return

      resultQuery = result['Value']

    done = False
    while not done:
      OK = True
      for field in fields:
        value = raw_input('%s [%s]: ' % (field,resultQuery[field]) )
        if value:
          resultQuery[field] = value

      print "\nResulting query:"
      printDict(resultQuery)
      print

      # Do some basic verification of the values
      if resultQuery['SimulationConditions'] != "All" and resultQuery['DataTakingConditions'] != "All":
        print "SimulationConditions and DataTakingConditions can not be defined simultaneously !"
        OK = False
#      if resultQuery['SimulationConditions'] == "All" and resultQuery['DataTakingConditions'] == "All":
#        print "Either SimulationConditions or DataTakingConditions must be defined !"
#        OK = False
      if resultQuery['ProcessingPass'] == "No default!":
        print "ProcessingPass must be defined !"
        OK = False
      for par in ['EventType','ProductionID']:
        try:
          dummy = int(resultQuery[par])
          resultQuery[par] = dummy
        except:
          print "%s must be integer" % par
          OK = False

      if OK:
        value = raw_input("OK,[A]bort,[R]etry:  [OK]:")
      else:
        value = raw_input("[A]bort,[R]etry:  [R]:")
        if not value:
          value = 'r'

      if not value and OK:
        result = self.server.addBookkeepingQuery(resultQuery)
        if not result['OK']:
          print "Query definition failed: ",result['Message']
        else:
          print "Query ID: ",result['Value']
        done = True
      elif value.lower() == 'a':
        done = True
      elif value.lower() == 'r':
        pass
      else:
        done = True

  #################################### JOBS SECTION ########################################################

  def do_addJob(self, args):
    """ Add single job to the Production

    Usage: addJob <ProductionNameOrID> [inputVector] [SE]
      <ProductionNameOrID> - Production Name or ID
      [inputVector] - input vector for Job
      [SE} - storage element to use for submission
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    if length > 1:
      vector = argss[1]
    else:
      vector = ''
    if length > 2:
      se = argss[2]
    else:
      se = ''
    self.server.addProductionJob(prodID, vector, se)

  def do_getJobInfo(self, args):
    """Returns information about specified job

      Usage: getJobInfoo <ProductionNameOrID> <JobID>
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    jobID = long(argss[1])
    ret = self.server.getJobInfo(prodID, jobID)
    if ret['OK']:
        val = ret['Value']
        print 'Production =', prodID,
        print 'JobID =', jobID
        print 'JobWmsID =',val['JobWmsID']
        print 'WmsStatus =',val['WmsStatus']
        print 'TargetSE =',val['TargetSE']
        print 'CreationTime =',val['CreationTime']
        print 'LastUpdateTime =',val['LastUpdateTime']
        print 'Input vector =', val['InputVector']
    else:
        print ret


  def do_deleteJobs(self, args):
    """ Delete jobs from the Production

    Usage: deleteJob ProductionNameOrID JobID
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    jobIDmin = long(argss[1])
    if length > 2:
      jobIDmax = long(argss[2])
    else:
      jobIDmax = jobIDmin

    print self.server.deleteJobs(prodID, jobIDmin, jobIDmax)

  def do_submitJobs(self, args):
    """ Submit jobs given number of jobs of the specified Production

    Usage: submitJobs productionID [numJobs=1] [site]
    list of sites = LCG.CERN.ch LCG.CNAF.it LCG.PIC.es LCG.IN2P3.fr LCG.NIKHEF.nl LCG.GRIDKA.de LCG.RAL.uk DIRAC.CERN.ch
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    numJobs=int(1)
    site = ''
    if length>1:
      numJobs = int(argss[1])
    if length>2:
      site = argss[2]

    prod = DiracProduction()

    result = prod.submitProduction(prodID, numJobs, site)

  def do_testMode(self, args):
    """ Changes Production manager handler to the testing version

        /opt/dirac/slc4_ia32_gcc34/bin/python2.4 /afs/cern.ch/user/g/gkuznets/workspace/DIRAC3/DIRAC/Core/scripts/dirac-service ProductionManagement/serverTest -o LogLevel=debug
    """

    self.serverUrl = 'dips://volhcb03.cern.ch:9138/ProductionManagement/ProductionManagerTest'
    self.server=RPCClient(self.serverUrl)
    self.prompt = '(Test)'
