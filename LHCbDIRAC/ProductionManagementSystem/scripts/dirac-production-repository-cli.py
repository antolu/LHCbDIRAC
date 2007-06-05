# $Id: dirac-production-repository-cli.py,v 1.8 2007/06/05 15:06:50 gkuznets Exp $
__RCSID__ = "$Revision: 1.8 $"

import cmd
import sys
import signal

import os, new
def getuid():
  return 15614
os.getuid=getuid



from DIRAC.Core.Base import Script
from DIRAC.Core.Base.Script import localCfg
from DIRAC.ProductionManagementSystem.Client.ProductionRepositoryClient import ProductionRepositoryClient
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC.LoggingSystem.Client.Logger import gLogger

localCfg.addDefaultEntry("LogLevel", "DEBUG")

gLogger._minLevel=30
gLogger.error("bla bla bla")
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

  def do_publish(self, args):
    """
    Publish Workflow in the repository
      Usage: publish <filename>
      <filename> is a path to the file with the xml description of the workflow
    """
    self.repository.publishWorkflow(args)

if __name__=="__main__":
    cli = ProductionRepositoryCLI()
    cli.cmdloop()
    #print gConfig.getValue( "/DIRAC/Setup", "Production" )
