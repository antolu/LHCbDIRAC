# $Id: dirac-production-manager-cli.py,v 1.8 2008/02/19 09:50:55 gkuznets Exp $
__RCSID__ = "$Revision: 1.8 $"

import cmd
import sys, os
import signal
import string
#import os, new

from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import localCfg
#from DIRAC.Core.Utilities.ColorCLI import colorize
from DIRAC import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient
#job submission
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job   import Job
from DIRAC.Interfaces.API.DiracProduction   import DiracProduction
from DIRAC.Core.Workflow.Workflow import *

from DIRAC.Core.Workflow.WorkflowReader import *

localCfg.addDefaultEntry("LogLevel", "DEBUG")
#gLogger._minLevel=30
Script.parseCommandLine() ## call to Script

class ProductionManagerCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.identSpace = 20
    self.productionManagerUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionManager')
    self.productionManager=RPCClient(self.productionManagerUrl)

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
    numJobs=1
    argss = string.split(args)
    site = ''
    prodID = long(argss[0])
    if len(argss)>1:
      numJobs = int(argss[1])      
    if len(argss)>2:
      site = argss[2]
      
    #wms = Dirac()
    prod = DiracProduction()

    
    result2 = self.productionManager.getJobsWithStatus(prodID, 'CREATED', numJobs, site)
    if not result2['OK']:
      print "Error during command execution: %s" % result2['Message']
      return
        
    job_counter=0;
    jobDict = result2["Value"]
    if jobDict == {}:
      print "Coild not get", numJobs, "jobs for site ", site
      return
    
    result1 = self.productionManager.getProductionBodyByID(prodID)
    if not result1['OK']:
      print "Error during command execution: %s" % result1['Message']
      return
    body = result1["Value"]
    #print body

    for jobid_ in jobDict:
      jobID = long(jobid_)
      jfilename = prod._DiracProduction__createJobDescriptionFile(body)
      job = Job(jfilename)
      #job = Job(body)
      job.workflow.setValue("JOB_ID",self.__convertIDtoString(jobID))
      job.workflow.setValue("PRODUCTION_ID",self.__convertIDtoString(prodID))
      for paramName in jobDict[jobID]:
        job.workflow.setValue(paramName,jobDict[jobID][paramName])
      job.setName(self.__convertIDtoString(prodID)+'_'+self.__convertIDtoString(jobID))
      
      result3 = prod._DiracProduction__getCurrentUser()
      if not result3['OK']:
        print result3,'Could not establish user ID from proxy credential or configuration'
	return
      userID=result3['Value']

      job.setOwner(userID)
      #job.setDestination(site) 
      updatedJob = prod._DiracProduction__createJobDescriptionFile(job._toXML())
      result4 = prod._DiracProduction__submitJob(job)
      if result4['OK']:
        jobWmsID = result4['Value']
        #update status in the  ProductionDB
        result5 = self.productionManager.setJobStatusAndWmsID(prodID, jobID, 'SUBMITTED', str(jobWmsID))
        if not result5['OK']:
          print "Could not change job status and WmsID in the ProductionDB"
          return
      else:
        print "Could not submit job %d of production %d with message=%s"%(prodID, jobID, result3['Message'])
        return
      prod._DiracProduction__cleanUp()
      job_counter=job_counter+1
      print "Loop:%d Production:%d JobID:%d submitted with WmsID:%d"%(job_counter, prodID, jobID, jobWmsID)
      

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
    Usage: setStatusPRid ProdName Status
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

    #if not self.lfc:
    #  from DIRAC.DataMgmt.FileCatalog.LcgFileCatalogClient import LcgFileCatalogClient
    #  self.lfc = LcgFileCatalogClient()

    #start = time.time()
    #result = self.lfc.getPfnsInDir(directory)
    #end = time.time()
    #print "getPfnsInDir",directory,"operation time",(end-start)

    #lfns = []
    #if result['Status'] == 'OK':
    #  lfndict = result['Replicas']
    #  for lfn,repdict in lfndict.items():
    #    for se,pfn in repdict.items():
    #      lfns.append((se,lfn))

    #result = self.productionManager.addFiles(lfns,force)
    #if result['Status'] != "OK":
    #  print "Failed to add files with local LFC interrogation"
    #  print "Trying the addDirectory on the Server side"
    #else:
    #  print result['Message']
    #  return

    # Local file addition failed, try the remote one
    result = self.productionManager.addDirectory(directory)
    print result
    if result['OK']:
      print result['Message']
    else:
      print result['Value']
      
  def do_getProductionInfo(self, args):
    """
    Delete Production from the the repository
      Usage: getProductionInfo Production
    """
    prodID = long(args)
    print self.productionManager.getProductionInfo(prodID)

      
  def do_test(self, args):
    """ Testing function for Gennady
    """    
    prodID = long(args)

    print self.productionManager.getJobStats(prodID)

if __name__=="__main__":
    cli = ProductionManagerCLI()
    cli.cmdloop()
