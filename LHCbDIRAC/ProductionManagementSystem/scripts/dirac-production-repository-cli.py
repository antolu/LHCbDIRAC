# $Id: dirac-production-repository-cli.py,v 1.12 2007/11/13 20:35:06 gkuznets Exp $
__RCSID__ = "$Revision: 1.12 $"

import cmd
import sys
import signal

import os, new
#def getuid():
#  return 15614
#os.getuid=getuid

from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import localCfg
from DIRAC.ProductionManagementSystem.Client.ProductionRepositoryClient import ProductionRepositoryClient
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger

localCfg.addDefaultEntry("LogLevel", "DEBUG")

gLogger._minLevel=30
Script.parseCommandLine()

class ProductionRepositoryCLI( cmd.Cmd ):

  def __init__( self ):
    cmd.Cmd.__init__( self )
    self.identSpace = 20
    self.repository = ProductionRepositoryClient()

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

  def do_wf_publish(self, args):
    """
    Publish Workflow in the repository
      Usage: wf_publish <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, publishing will be refused.
    """
    self.repository.publishWorkflow(args, False)

  def do_wf_delete(self, args):
    """
    Delete Workflow from the the repository
      Usage: wf_delete <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, publishing will be refused.
    """
    self.repository.deleteWorkflow(args)

  def do_wf_update(self, args):
    """
    Publish or Update Workflow in the repository
      Usage: wf_update <filename>
      <filename> is a path to the file with the xml description of the workflow
      If workflow already exists, it will be replaced.
    """
    self.repository.publishWorkflow(args, True)

  def do_wf_list(self, args):
    """
    List all Workflows in the repository
      Usage: wf_list
    """
    ret = self.repository.getListWorkflows()
    if not ret['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      print ret['Value']
      print "----------------------------------------------------------------------------------"
      print "|    Name    |   Parent   |         Time        |          DN          | Comment |"
      print "----------------------------------------------------------------------------------"
      for wf in ret['Value']:
        print "| %010s | %010s | %014s | %s | %s |" % (wf[0],'',wf[2],wf[1][wf[1].rfind('/CN=')+4:],'')
      print "----------------------------------------------------------------------------------"

# Production part

  def do_pr_submit(self, args):
    """
    Submit Production to the repository
      Usage: pr_submit <filename>
      <filename> is a path to the file with the xml description of the workflow
      If production already exists, submission will be refused.
    """
    self.repository.submitProduction(args, False)

  def do_pr_update(self, args):
    """
    Update Production in the repository
      Usage: pr_update <filename>
      <filename> is a path to the file with the xml description of the workflow
      If production already exists, submission will be refused.
    """
    self.repository.submitProduction(args, True)

  def do_pr_list(self, args):
    """
    List all Productions in the repository
      Usage: pr_list
    """
    ret = self.repository.getListProductions()
    if not ret['OK']:
      print "Error during command execution: %s" % ret['Message']
    else:
      print "-----------------------------------------------------------------------------------------------------------------------------------------"
      print "|    ID    |    Name    |  Status  |   Parent   |   Total  | Submited |   Last   |         Time        |          DN          | Comment |"
      print "-----------------------------------------------------------------------------------------------------------------------------------------"
      for production in ret['Value']:
        print "| %08i | %010s | %08s | %010s | %08i | %08i | %08i | %014s | %s | %s |" % (production[0:8]+(production[8][production[8].rfind('/CN=')+4:],production[9]))
      print "-----------------------------------------------------------------------------------------------------------------------------------------"

if __name__=="__main__":
    cli = ProductionRepositoryCLI()
    cli.cmdloop()
    #print gConfig.getValue( "/DIRAC/Setup", "Production" )
