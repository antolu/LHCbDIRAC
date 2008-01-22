# $Id: dirac-production-repository-cli.py,v 1.17 2008/01/22 14:15:14 gkuznets Exp $
__RCSID__ = "$Revision: 1.17 $"

import cmd
import sys
import signal
import string

import os, new
#def getuid():
#  return 15614
#os.getuid=getuid

from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import localCfg
from DIRAC.ProductionManagementSystem.Client.ProductionRepositoryClient import ProductionRepositoryClient
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger
from DIRAC.Core.DISET.RPCClient import RPCClient

#job submission
from DIRAC.Interfaces.API.Dirac import Dirac
from DIRAC.Interfaces.API.Job                            import Job
from DIRAC.Core.Workflow.WorkflowReader import *

localCfg.addDefaultEntry("LogLevel", "DEBUG")

gLogger._minLevel=30
Script.parseCommandLine()

class ProductionRepositoryCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.identSpace = 20
    self.productionRepositoryUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionRepository')
    self.repository=RPCClient(self.productionRepositoryUrl)
    #self.repository = ProductionRepositoryClient()

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
    #if self.modifiedData:
    #  print "Changes are about to be written to file for later use."
    #  self.do_writeToFile( self.backupFilename )
    #  print "Changes written to %s.cfg" % self.backupFilename
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

  def do_wf_submit(self, args):
    """
    Publish Workflow in the repository
      Usage: wf_publish <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, publishing will be refused.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    result = self.repository.submitWorkflow(body, False)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_wf_update(self, args):
    """
    Publish or Update Workflow in the repository
      Usage: wf_update <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, it will be replaced.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    result = self.repository.submitWorkflow(body, True)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']


  def do_wf_get(self, args):
    """
    Read Workflow from the repository
      Usage: wf_get <WFName> <filename>
      <WFName> - the name of the workflow
      <filename> is a path to the file to write xml of the workflow
    """
    argss = string.split(args)
    wf_name = argss[0]
    path = argss[1]

    result = self.repository.getWorkflow(wf_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return

    body = result['Value']
    fd = open( path, 'wb' )
    fd.write(body)
    fd.close()

  def do_wf_delete(self, args):
    """
    Delete Workflow from the the repository
      Usage: wf_delete WorkflowName
    """
    result = self.repository.deleteWorkflow(args)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_wf_list(self, args):
    """
    List all Workflows in the repository
      Usage: wf_list
    """
    result = self.repository.getListWorkflows()
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
    else:
      print "----------------------------------------------------------------------------------"
      print "|    Name    |   Parent   |         Time        |          DN          | Comment |"
      print "----------------------------------------------------------------------------------"
      for wf in result['Value']:
        print "| %010s | %010s | %014s | %s | %s |" % (wf['WFName'],wf['WFParent'],wf['PublishingTime'],wf['PublisherDN'][wf['PublisherDN'].rfind('/CN=')+4:],wf['Description'])
      print "----------------------------------------------------------------------------------"

################ PRODUCTION SECTION ####################################

  def do_pr_submit(self, args):
    """
    Submit Production to the repository
      Usage: pr_submit <filename>
      <filename> is a path to the file with the xml description of the workflow
      If production already exists, submission will be refused.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    self.repository.submitProduction(body, False)

  def do_pr_update(self, args):
    """
    Update Production in the repository
      Usage: pr_update <filename>
      <filename> is a path to the file with the xml description of the workflow
      If production already exists, submission will be refused.
    """
    fd = file( args )
    body = fd.read()
    fd.close()
    self.repository.submitProduction(body, True)

  def do_pr_list(self, args):
    """
    List all Productions in the repository
      Usage: pr_list
    """
    ret = self.repository.getListProductions()
    if not ret['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      print "--------------------------------------------------------------------------------------------------------------------------------------------"
      print "|    ID    |    Name    |  Status  |   Parent   |   Total  | Submited |   Last   |         Time        |          DN          | Description |"
      print "--------------------------------------------------------------------------------------------------------------------------------------------"
      for pr in ret['Value']:
        print "| %08i | %010s | %08s | %010s | %08i | %08i | %08i | %014s | %s | %s |" % (pr["ProductionID"],
               pr['PRName'], pr['Status'], pr['PRParent'], pr['JobsTotal'],
               pr['JobsSubmitted'], pr['LastSubmittedJob'], pr['PublishingTime'], pr['PublisherDN'], pr['Description'])
      print "-----------------------------------------------------------------------------------------------------------------------------------------"


  def do_pr_delete(self, args):
    """
    Delete Production from the the repository
      Usage: pr_delete ProductionName
    """
    print self.repository.deleteProduction(args)

  def do_pr_deleteID(self, args):
    """
    Delete Production from the the repository
      Usage: pr_deleteID ProductionID
      argument is an integer
    """
    print self.repository.deleteProductionID( int(args) )

  def do_pr_get(self, args):
    """
    Read Production from the repository
      Usage: pr_get <PRName> <filename>
      <PRName> - the name of the production
      <filename> is a path to the file to write xml of the workflow
    """
    argss = string.split(args)
    pr_name = argss[0]
    path = argss[1]

    body = self.repository.getProduction(pr_name)['Value']
    fd = open( path, 'w' )
    fd.write(body)
    fd.close()

  def do_pr_getID(self, args):
    """
    Read Production from the repository
      Usage: pr_getID <ProductionID> <filename>
      <ID> - ID of the production
      <filename> is a path to the file to write xml of the workflow
    """
    argss = string.split(args)
    id = int(argss[0])
    path = argss[1]

    body = self.repository.getProductionID(id)['Value']
    fd = open( path, 'w' )
    fd.write(body)
    fd.close()

  def do_pr_info(self, args):
    """
    Reads information about Production from the repository
      Usage: pr_info <PRName>
      <PRName> - the name of the production
    """
    argss = string.split(args)
    pr_name = argss[0]

    result = self.repository.getProductionInfo(pr_name)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    pr = result['Value']
    print "| %08i | %010s | %08s | %010s | %08i | %08i | %08i | %014s | %s | %s |" % (pr["ProductionID"],
               pr['PRName'], pr['Status'], pr['PRParent'], pr['JobsTotal'],
               pr['JobsSubmitted'], pr['LastSubmittedJob'], pr['PublishingTime'], pr['PublisherDN'], pr['Description'])

  def do_pr_infoID(self, args):
    """
    Reads information about Production from the repository
      Usage: pr_infoID <productionID>
      <ID> - ID of the production
    """
    argss = string.split(args)
    id = int(argss[0])

    result = self.repository.getProductionInfoID(id)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    pr = result['Value']
    print "| %08i | %010s | %08s | %010s | %08i | %08i | %08i | %014s | %s | %s |" % (pr["ProductionID"],
               pr['PRName'], pr['Status'], pr['PRParent'], pr['JobsTotal'],
               pr['JobsSubmitted'], pr['LastSubmittedJob'], pr['PublishingTime'], pr['PublisherDN'], pr['Description'])

  def do_jobs_submit_local(self, args):
    """
    Submit N Jobs of the production
      Usage: jobs_submit ProductionID NJobs
      ProductionID - is the integer
      NJobs - number of jobs to submit
    WARNING!!! This is a temporary solution while I am on holiday from 17/11 to 26/11
    Joel I am sory for the limited functionality

    Please see Stuart about default JDL parameters in the workflow
    whey are not decided yet
    self._addParameter(self.workflow,'Owner','JDL',self.owner,'Job Owner')
    self._addParameter(self.workflow,'JobType','JDL',self.type,'Job Type')
    self._addParameter(self.workflow,'Priority','JDL',self.priority,'User Job Priority')
    self._addParameter(self.workflow,'JobGroup','JDL',self.group,'Corresponding VOMS role')
    self._addParameter(self.workflow,'JobName','JDL',self.name,'Name of Job')
    self._addParameter(self.workflow,'DIRACSetup','JDL',self.setup,'DIRAC Setup')
    self._addParameter(self.workflow,'Site','JDL',self.site,'Site Requirement')
    self._addParameter(self.workflow,'Origin','JDL',self.origin,'Origin of client')
    self._addParameter(self.workflow,'StdOutput','JDL',self.stdout,'Standard output file')
    self._addParameter(self.workflow,'StdError','JDL',self.stderr,'Standard error file')
    """
    argss = string.split(args)

    id = int(argss[0])
    njobs = int(argss[1])
    wms = Dirac()

    result = self.repository.getProductionID(id)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']
      return
    body = result["Value"]
    while njobs > 0:
      #print body
      wf = fromXMLString(body)
      job = Job()
      job.workflow = wf
      job._dumpParameters()
      job._Job__setJobDefaults()
      #stramge fix to avoid error
      job._addParameter(job.workflow,'requirements','JDL','','Do not know what to add')

      result = wms.submit(job)
      if not result['OK']:
        print "Can not submit job bacause %s" % result['Message']
      njobs=njobs-1
      print result


if __name__=="__main__":
    cli = ProductionRepositoryCLI()
    cli.cmdloop()
    #print gConfig.getValue( "/DIRAC/Setup", "Production" )
