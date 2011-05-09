# $HeadURL$
"""List configuration of LHCbDIRAC T0-T1 FTS channels.
"""

__RCSID__ = "$Id$"

import sys
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Grid import executeGridCommand

gridEnv = "/afs/cern.ch/project/gd/LCG-share/3.2.8-0/etc/profile.d/grid-env"
Tier1s = [ "GRIDKA", "NIKHEF", "PIC", "CNAF", "IN2P3", "CERN", "RAL",
                      "SARA", "INFN", "FZK", "CERNPROD", "RALLCG2", "FZKLCG2",
                      "SARAMATRIX", "INFNT1", "IN2P3CC"  ]
Tier1sChannels = [ chA+'-'+chB for chA in Tier1s for chB in Tier1s ]
translate = { "CERNPROD" : "CERN", "FZK" : "GRIDKA",
              "INFNT1" : "CNAF", "FZKLCG2" : "GRIDKA",
              "IN2P3CC" : "IN2P3", "RALLCG2" : "RAL",
              "SARAMATRIX" : "SARA" }

def getChannel( ch ):
  """ translate channel name from grid to lhcb world
  """
  source, target = ch.split("-")
  if source in translate: source = translate[source]
  if target in translate: target = translate[target]
  return "%s-%s" % (source, target)

def extract( stdout ):
  """
  extract info about perticular channel directly from command output
  """  
  name = origName = state = band = files = streams = "" 
  for line in stdout.split("\n"):
    if line.startswith("Channel:"):
      origName = line.split(":")[1].strip()
      name = getChannel( origName )
    elif line.startswith("State:"):
      state = line.split(":")[1].strip()
    elif line.startswith("Bandwidth:"):
      band = line.split(":")[1].strip()
    elif line.startswith("VO 'lhcb'"):
      share = line.split(":")[1].strip()
    elif line.startswith("Number of files"):
      words = line.split()
      files = words[3][:-1]
      streams = words[5]
  return name, origName, files, streams, share, state, band
  
def execute():
  """
  main worker here
  """
  usageStr = __doc__ + """
Usage:
	%s [option|cfgfile] 
""" % Script.scriptName
  Script.setUsageMessage( usageStr )
  Script.parseCommandLine()
  import DIRAC
  
  res = DIRAC.gConfig.getOptionsDict( "/Resources/FTSEndpoints")
  if not res["OK"]:
    return res["Message"] 

  URL2FTS = dict( [ (k, v.replace("FileTransfer", "ChannelManagement") ) for (k, v) in res["Value"].items() ]  )
  FTS2URL = dict( [ (v, k) for (k, v) in URL2FTS.items() ] )

  head = " %-18s %-18s %-10s %-10s %-10s %-10s %-10s" % ( "Name", "OrigName", "Files", "Streams", "Share", "State", "Band" )
  hashLine = "#" * len(head)
    
  for ftsLCG, ftsURL in sorted(URL2FTS.items()):
    print hashLine
    print "# %s (%s)" % ( ftsLCG, ftsURL ) 
    print head
    command = ["glite-transfer-channel-list", "-s",  ftsURL ]
    res = executeGridCommand( "",  command, gridEnv )
    if not res["OK"]:
      return res["Message"]
    res = res["Value"]
    exitCode, stdout, stderr = res
    if exitCode:
      return stderr
    channels = stdout.split()
    for channel in sorted(channels):
      if channel not in Tier1sChannels: continue
      command = ["glite-transfer-channel-list", "-s", ftsURL, channel ]
      res = executeGridCommand( "",  command, gridEnv )
      if not res["OK"]:
        return res["Message"]
      res = res["Value"]
      exitCode, stdout, stderr = res
      if "VO 'lhcb'" in stdout:
        print " %-18s %-18s %-10s %-10s %-10s %-10s %-10s" % extract( stdout ) 

## entry point
if __name__ == "__main__":
  sys.exit( execute() )
