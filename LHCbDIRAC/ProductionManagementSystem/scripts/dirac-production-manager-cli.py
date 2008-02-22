# $Id: dirac-production-manager-cli.py,v 1.14 2008/02/22 15:52:36 gkuznets Exp $
__RCSID__ = "$Revision: 1.14 $"

import cmd
import sys, os
import signal
import string
import time
#import os, new
from DIRAC import S_OK, S_ERROR
from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import localCfg
#from DIRAC.Core.Utilities.ColorCLI import colorize
from DIRAC import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
#job submission
from DIRAC.Interfaces.API.DiracProduction   import DiracProduction

localCfg.addDefaultEntry("LogLevel", "DEBUG")
#gLogger._minLevel=30
Script.parseCommandLine() ## call to Script

class ProductionManagerCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.identSpace = 20
    self.productionManagerUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionManager')
    self.productionManager=RPCClient(self.productionManagerUrl)
    self.lfc = None

  def printPair( self, key, value, separator=":" ):
    valueList = value.split( "\n" )
    print "%s%s%s %s" % ( key, " " * ( self.identSpace - len( key ) ), separator, valueList[0].strip() )
    for valueLine in valueList[ 1:-1 ]:
      print "%s  %s" % ( " " * self.identSpace, valueLine.strip() )

  def do_quit( self, *args ):
    """
    Exits the application
        Usage: quit
    """
    sys.exit( 0 )

  def do_help( self, args ):
    """
    Shows help information
        Usage: help <command>
        If no command is specified all commands are shown
    """
    if len( args ) == 0:
      print "\nAvailable commands:\n"
      attrList = dir( self )
      attrList.sort()
      for attribute in attrList:
        if attribute.find( "do_" ) == 0:
          self.printPair( attribute[ 3: ], getattr( self, attribute ).__doc__[ 1: ] )
          print ""
    else:
      command = args.split()[0].strip()
      try:
        obj = getattr( self, "do_%s" % command )
      except:
        print "There's no such %s command" % command
        return
      self.printPair( command, obj.__doc__[1:] )


  def check_params(self, args, num):
    """Checks if the number of parameters correct"""
    argss = string.split(args)
    length = len(argss)
    if length < num:
      print "Error: Number of arguments provided %d less that required %d, please correct." % (length, num)
      return (False, length)
    return (argss,length)


################ WORKFLOW SECTION ####################################

  def do_uploadWF(self, args):
    """
    Upload Workflow in the repository
      Usage: uploadWF <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, publishing will be refused.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    result = self.productionManager.publishWorkflow(body, False)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_updateWF(self, args):
    """
    Publish or Update Workflow in the repository
      Usage: updateWF <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, it will be replaced.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    result = self.productionManager.publishWorkflow(body, True)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']


  def do_getWF(self, args):
    """
    Read Workflow from the repository
      Usage: getWF <WFName> <filename>
      <WFName> - the name of the workflow
      <filename> is a path to the file to write xml of the workflow
    """
    argss = string.split(args)
    wf_name = argss[0]
    path = argss[1]

    result = self.productionManager.getWorkflow(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return

    body = result['Value']
    fd = open( path, 'wb' )
    fd.write(body)
    fd.close()

  def do_deleteWF(self, args):
    """
    Delete Workflow from the the repository
      Usage: deleteWF WorkflowName
    """
    result = self.productionManager.deleteWorkflow(args)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_listWF(self, args):
    """
    List all Workflows in the repository
      Usage: listWF
    """
    result = self.productionManager.getListWorkflows()
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
    else:
      print "----------------------------------------------------------------------------------"
      print "|    Name    |   Parent   |         Time        |          DN          |   Group   | Short Description |   Long Description   |"
      print "----------------------------------------------------------------------------------"
      for wf in result['Value']:
        print "| %010s | %010s | %014s | %s | %s | %s | %s |" % (wf['WFName'],wf['WFParent'],wf['PublishingTime'],wf['AuthorDN'][wf['AuthorDN'].rfind('/CN=')+4:],
                                                       wf['AuthorGroup'],wf['Description'],wf['LongDescription'])
      print "----------------------------------------------------------------------------------"

################ PRODUCTION SECTION ####################################

  def do_uploadPR(self, args):
    """
    Upload Production in to the transformation table
      Usage: uploadPR <filename> <filemask> <groupsize>
      <filename> is a path to the file with the xml description of the workflow
      If transformation with this name already exists, publishing will be refused.
      <filemask> mask to match files going to be accepted by transformation
      <groupsize> how many files going to be grouped per job
      WARNING!!! if <filemask> and <groupsize> are absent, the system will create 'SIMULATION' type of transformation
    """
    tr_mask = ''
    tr_groupsize = 0
    argss = string.split(args)
    tr_file = argss[0]
    if len(argss)>1:
      tr_mask = argss[1]
    else:
      tr_mask = ''
    if len(argss)>2:
      tr_groupsize = int(argss[2])

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.productionManager.publishProduction(body, tr_mask, tr_groupsize, False)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file

  def do_updatePR(self, args):
    """
    Replace Production in the transformation table even old one with this name exists
      Usage: updatePR <filename> <filemask> <groupsize>
      <filename> is a path to the file with the xml description of the workflow
      If transformation with this name already exists, publishing will be refused.
      <filemask> mask to match files going to be accepted by transformation
      <groupsize> how many files going to be grouped per job
      WARNING!!! if <filemask> and <groupsize> are absent, the system will create 'SIMULATION' type of transformation
    """
    tr_mask = ''
    tr_groupsize = 0
    argss = string.split(args)
    tr_file = argss[0]
    if len(argss)>1:
      tr_mask = argss[1]
    else:
      tr_mask = ''
    if len(argss)>2:
      tr_groupsize = int(argss[2])

    if os.path.exists(tr_file):
      fd = file( tr_file )
      body = fd.read()
      fd.close()
      result = self.productionManager.publishProduction(body, tr_mask, tr_groupsize, True)
      if not result['OK']:
        print "Error during command execution: %s" % result['Message']
    else:
      print "File %s does not exists" % tr_file

  def do_getPR(self, args):
    """
    Read Workflow from the repository
      Usage: getPR <PRName> <filename>
      <PRName> - the name of the workflow
      <filename> is a path to the file to write xml of the workflow
    """
    argss = string.split(args)
    wf_name = argss[0]
    path = argss[1]

    result = self.productionManager.getProductionBody(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return

    body = result['Value']
    fd = open( path, 'wb' )
    fd.write(body)
    fd.close()

  def do_deletePR(self, args):
    """
    Delete Production from the the repository
      Usage: deletePR Production Name
    """
    print self.productionManager.deleteProduction(args)


  def do_deletePRid(self, args):
    """
    Delete Production from the the repository
      Usage: deletePRid Production
    """
    prodID = long(args)
    print self.productionManager.deleteProductionByID(prodID)

  def do_listPR(self, args):
    """
    List all Productions in the repository
      Usage: listPR
    """
    ret = self.productionManager.getAllProductions()
    if not ret['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      print "--------------------------------------------------------------------------------------------------------------------------------------------"
      print "|    ID    |    Name    |  Status  |   Parent   |   Total  | Submited |   Last   |         Time        |          DN          | Description |"
      print "--------------------------------------------------------------------------------------------------------------------------------------------"
      for pr in ret['Value']:
        print "| %08s | %010s | %08s | %010s | %08s | %08s | %08s | %014s | %s | %s | %s | %s | %s | %s |" % (pr["TransID"],
               pr['Name'], pr['Parent'], pr['Description'], pr['LongDescription'], pr['CreationDate'],
               pr['AuthorDN'], pr['AuthorGroup'], pr['Type'], pr['Plugin'], pr['AgentType'], pr['Status'], pr['FileMask'], pr['GroupSize'])
      print "-----------------------------------------------------------------------------------------------------------------------------------------"

  #def do_addProdJob(self, args):
  #  """ Add single job to the Production
  #  Usage: addProdJob ProductionID [inputVector]
  #  """
  #  argss = string.split(args)
  #  prodID = long(argss[0])
  #  if len(argss)>1:
  #    vector = argss[1]
  #  else:
  #    vector = ''
  #  print self.productionManager.addProductionJob(prodID, vector)

  def __convertIDtoString(self,id):
    return ("%08d" % (id) )

  def do_submitJobs(self, args):
    """ Submit jobs given number of jobs of the specified Production
    Usage: addProdJob productionID [numJobs=1] [site]
    list of sites = LCG.CERN.ch LCG.CNAF.it LCG.PIC.es LCG.IN2P3.fr LCG.NIKHEF.nl LCG.GRIDKA.de LCG.RAL.uk DIRAC.CERN.ch
    """
    numJobs=int(1)
    argss = string.split(args)
    site = ''
    prodID = long(argss[0])
    if len(argss)>1:
      numJobs = int(argss[1])
    if len(argss)>2:
      site = argss[2]

    prod = DiracProduction()

    result = prod.submitProduction(prodID, numJobs, site)

  def do_setStatusID(self, args):
    """ Set status of the production
    Usage: setStatusPRid ProductionID Status
      New - newly created, equivalent to STOPED
      Active - can submit
      Flush - final stage, ignoring GroupSize
      Stopped - stopped by manager
      Done - job limits reached, extension is possible
      Error - Production with error, equivalent to STOPPED
      Terminated - stopped, extension impossible
      Case of the letters will be ignored
    """
    argss = string.split(args)
    prodID = long(argss[0])
    status = argss[1]
    self.productionManager.setProductionStatusByID(prodID, status)

  def do_setStatus(self, args):
    """ Set status of the production
    Usage: setStatusPR ProdName Status
      New - newly created, equivalent to STOPED
      Active - can submit
      Flush - final stage, ignoring GroupSize
      Stopped - stopped by manager
      Done - job limits reached, extension is possible
      Error - Production with error, equivalent to STOPPED
      Terminated - stopped, extension impossible
      Case of the letters will be ignored
    """
    argss = string.split(args)
    prodName = argss[0]
    status = argss[1]
    self.productionManager.setProductionStatus(prodName, status)

  def do_setSubmissionType(self, args):
    """ Set status of the production
    Usage: setSubmissionType ProdName SubmissionStatus
      Manual - submission by production manager only
      Automatic - submission by ProductionJobAgent
    """
    argss = string.split(args)
    prodName = argss[0]
    status = argss[1]
    self.productionManager.setTransformationAgentType(prodName, status)

  def do_setSubmissionTypeID(self, args):
    """ Set status of the production
    Usage: setSubmissionType ProdName SubmissionStatus
      Manual - submission by production manager only
      Automatic - submission by ProductionJobAgent
    """
    argss = string.split(args)
    id_ = long(argss[0])
    status = argss[1]
    self.productionManager.setTransformationAgentType(id_, status)

  def do_setTransformationMaskID(self, args):
    """ Overrites transformation mask for the production
    Usage: setTransformationMaskID ProductionID Mask
    """
    argss = string.split(args)
    prodID = long(argss[0])
    mask = argss[1]
    self.productionManager.setTransformationMaskID(prodID, mask)

  def do_setTransformationMask(self, args):
    """ Overrites transformation mask for the production
    Usage: setTransformationMaskID ProductionName Mask
    """
    argss = string.split(args)
    prodID = argss[0]
    mask = argss[1]
    self.productionManager.setTransformationMask(prodID, mask)

  def do_addDirectory(self,args):
    """Add files from the given catalog directory

    usage: addDirectory <directory> [force]
    """

    argss = string.split(args)
    directory = argss[0]
    force = 0
    if len(argss) == 2:
      if argss[1] == 'force':
        force = 1

    # KGG checking if directory has / at the end, if yes we remove it
    directory=directory.rstrip('/')

    if not self.lfc:
      from DIRAC.DataManagementSystem.Client.Catalog.LcgFileCatalogCombinedClient import LcgFileCatalogCombinedClient
      self.lfc = LcgFileCatalogCombinedClient()

    start = time.time()
    result = self.lfc.getDirectoryReplicas(directory)
    end = time.time()
    print "getPfnsInDir",directory,"operation time",(end-start)

    lfns = []
    if result['OK']:
      if 'Successful' in result['Value'] and directory in result['Value']['Successful']:
        lfndict = result['Value']['Successful'][directory]

        for lfn,repdict in lfndict.items():
          for se,pfn in repdict.items():
            lfns.append((lfn,pfn,0,se,'IGNORED-GUID','IGNORED-CHECKSUM'))

        result = self.productionManager.addFile(lfns, False)

        if not result['OK']:
          print "Failed to add files with local LFC interrogation"
          print "Trying the addDirectory on the Server side"
        else:
          print "Operation successfull"
          return
      else:
        print "No such directory in LFC"

    # Local file addition failed, try the remote one
    #result = self.productionManager.addDirectory(directory)
    #print result
    #if not result['OK']:
    #  print result['Message']
    #else:
    #  print result['Value']

  def do_getProductionInfo(self, args):
    """
    Returns information about production
      Usage: getProductionInfo Production
    """
    prodID = long(args)
    #print self.productionManager.getProductionInfo(prodID)
    print self.productionManager.getJobStats(prodID)

  def do_getJobInfo(self, args):
    """
    Returns information about production
      Usage: getJobInfoo ProductionID JobID
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = long(argss[0])
    jobID = long(argss[1])
    print self.productionManager.getJobInfo(prodID, jobID)


  def do_testMode(self, args):
    """ Test mode changes Production manager handler to the testing version
    """
    self.productionManagerUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionManagerTest')
    self.productionManager=RPCClient(self.productionManagerUrl)
    self.prompt = '(Test)'

if __name__=="__main__":
    cli = ProductionManagerCLI()
    cli.cmdloop()
