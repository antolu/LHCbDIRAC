# $Id: dirac-production-manager-cli.py,v 1.18 2008/02/28 17:26:09 gkuznets Exp $
__RCSID__ = "$Revision: 1.18 $"

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
from DIRAC.Core.Transformation.TransformationDBCLI   import TransformationDBCLI
#job submission
from DIRAC.Interfaces.API.DiracProduction   import DiracProduction

localCfg.addDefaultEntry("LogLevel", "DEBUG")
#gLogger._minLevel=30
Script.parseCommandLine() ## call to Script

class ProductionManagerCLI( TransformationDBCLI ):

  def __init__( self ):
    TransformationDBCLI.__init__( self )
    self.identSpace = 20
    self.serverUrl = gConfig.getValue('/Systems/ProductionManagement/Development/URLs/ProductionManager')
    self.server=RPCClient(self.serverUrl)
    self.lfc = None

  def stripDN(self, dn):
      df.rfind

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
    result = self.server.publishWorkflow(body, False)
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
    result = self.server.publishWorkflow(body, True)
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

    result = self.server.getWorkflow(wf_name)
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
    result = self.server.deleteWorkflow(args)
    if not result['OK']:
      print "Error during command execution: %s" % result['Message']

  def do_listWF(self, args):
    """
    List all Workflows in the repository
      Usage: listWF
    """
    result = self.server.getListWorkflows()
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

  def do_getPRBody(self, args):
    """
    Read Workflow from the repository
      Usage: getPRBody <ProductionNameOrID> <filename>
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

  def do_setPRBody(self, args):
    """
    Upload new body for the production
      Usage: setPRBody <ProductionNameOrID> <filename>
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

  def do_deletePR(self, args):
    """
    Delete Production from the the repository
      Usage: deletePR <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    print self.server.deleteProduction(prodID)

  def do_listPR(self, args):
    """
    List all Productions in the repository
      Usage: listPR
    """
    ret = self.server.getAllProductions()
    if not ret['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      print "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
      print "| ID |    Name    |  Parent  |   Description   |   LongDescription  | CreationDate |   Author   | AuthorGroup |  Type    | Plugin | AgentType | Status | FileMask | GroupSize |"
      print "------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
      for pr in ret['Value']:
        print "| %06s | %010s | %08s | %010s | %08s | %08s | %08s | %014s | %s | %s | %s | %s | %s | %s |" % (pr["TransID"],
               pr['Name'], pr['Parent'], pr['Description'], pr['LongDescription'], pr['CreationDate'],
               pr['AuthorDN'], pr['AuthorGroup'], pr['Type'], pr['Plugin'], pr['AgentType'], pr['Status'], pr['FileMask'], pr['GroupSize'])
      print "-----------------------------------------------------------------------------------------------------------------------------------------"

  def do_getPRInfo(self, args):
    """
    Returns information about production
      Usage: getProductionInfo <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    #print self.server.getProductionInfo(prodID)
    print self.server.getJobWmsStats(prodID)
    print self.server.getJobStats(prodID)

  def do_setPRStatus(self, args):
    """ Set status of the production
    Usage: setPRStatus ProdName Status
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

  def do_getPRLog(self, args):
    """ Get Log of the given ProductionID
    Usage: getPRLog <ProductionNameOrID>
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
      print mess['Message'], mess['MessageDate'], stripDN( mess['AuthorDN'])

  def do_setPRSubmissionType(self, args):
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

  def do_setPRMask(self, args):
    """ Overrites transformation mask for the production
    Usage: setPRMask <ProdNameOrID> <Mask>
    """
    argss, length = self.check_params(args, 2)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    mask = argss[1]
    self.server.setTransformationMask(prodID, mask)

  def do_updateProduction(self, args):
    """ Get Log of the given ProductionID
    Usage: getPRLog <ProductionNameOrID>
    """
    argss, length = self.check_params(args, 1)
    if not argss:
      return
    prodID = self.check_id_or_name(argss[0])
    print self.server.updateTransformation(prodID)


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
    """
    Returns information about specified job
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
    Usage: addProdJob productionID [numJobs=1] [site]
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

  def do_addDirectory(self,args):
    """Add files from the given catalog directory

    usage: addDirectory <directory> [force]
    """

    argss, length = self.check_params(args, 1)
    if not argss:
      return
    directory = argss[0]
    force = 0
    if length > 1:
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

        result = self.server.addFile(lfns, False)

        if not result['OK']:
          print "Failed to add files with local LFC interrogation"
          print "Trying the addDirectory on the Server side"
        else:
          print "Operation successfull"
          file_exists = 0
          forced = 0
          pass_filter = 0
          retained = 0
          replica_exists = 0
          added_to_calalog = 0
          added_to_transformation = 0
          total = len(result['Value']['Successful'])
          failed = len(result['Value']['Failed'])
          for fn in result['Value']['Successful']:
            f = result['Value']['Successful'][fn]
            if f['FileExists']:
                file_exists = file_exists+1
            if f['Forced']:
                forced = forced+1
            if f['PassFilter']:
                pass_filter = pass_filter+1
            if f['Retained']:
                retained = retained+1
            if f['ReplicaExists']:
                replica_exists = replica_exists+1
            if f['AddedToCatalog']:
                added_to_calalog = added_to_calalog+1
            if f['AddedToTransformation']:
                added_to_transformation = added_to_transformation+1

          print 'Failed:',  failed
          print 'Successful:', total
          print 'Pass filters', pass_filter
          print 'Forced in:', forced
          print 'Pass filters + forced = Retained:', retained
          print 'Exists in Catalog', file_exists
          print 'Added to Catalog', added_to_calalog-file_exists
          print 'Added to Transformations', added_to_transformation
          print 'Replica Exixts', replica_exists
          return
      else:
        print "No such directory in LFC"

    # Local file addition failed, try the remote one
    #result = self.server.addDirectory(directory)
    #print result
    #if not result['OK']:
    #  print result['Message']
    #else:
    #  print result['Value']


  def do_testMode(self, args):
    """ Changes Production manager handler to the testing version
        /opt/dirac/slc4_ia32_gcc34/bin/python2.4 /afs/cern.ch/user/g/gkuznets/workspace/DIRAC3/DIRAC/Core/scripts/dirac-service ProductionManagement/serverTest -o LogLevel=debug
    """

    self.serverUrl = 'dips://volhcb03.cern.ch:9138/ProductionManagement/ProductionManagerTest'
    self.server=RPCClient(self.serverUrl)
    self.prompt = '(Test)'

if __name__=="__main__":
    cli = ProductionManagerCLI()
    cli.cmdloop()
