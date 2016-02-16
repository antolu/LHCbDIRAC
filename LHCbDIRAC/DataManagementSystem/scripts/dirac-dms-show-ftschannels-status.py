#!/bin/env python
"""Display FTS channels configuration used by DIRAC to transfer files between different Tiers.
"""

outputInfo = """
Output:
 [1] Name:        name used by VO (carefull, it's lhcb specific)
 [2] OrigName:    name of channel set during channel creation
 [3] Files:       number of concurrent file transfers (other will be enqueued)
 [4] Streams:     number of concurrent streams in each transfer
 [5] Share:       VO share:
                  - lower bound: your VO only 
                  - upper bound: sum of all shares including your VO          
 [6] State:       operation state of the channel (one of Active, Inactive, Drain, 
                  Halted, Stopped, Archived) 
 [7] Bandwidth:   nominal bandwidth of the channel (in Mb/s)

 Please read man pages for glite-transfer-channel-add, glite-transfer-channel-list,
 glite-transfer-channel-set and glite-transfer-channel-setvoshare for more information.
"""

__RCSID__ = "$Id: dirac-dms-show-ftschannels-status.py 69359 2013-08-08 13:57:13Z phicharp $"

import sys
from DIRAC.Core.Base import Script
from DIRAC.Core.Utilities.Grid import executeGridCommand

## grid env settings
gridEnv = "/afs/cern.ch/project/gd/LCG-share/3.2.8-0/etc/profile.d/grid-env"

## Tier1 (lhcb specific!)
Tier1s = [ "GRIDKA", "NIKHEF", "PIC", "CNAF", "IN2P3", "CERN", "RAL",
           "SARA", "INFN", "FZK", "CERNPROD", "RALLCG2", "FZKLCG2",
           "SARAMATRIX", "INFNT1", "IN2P3CC"  ]
## T0-T1, T1-T1 channels
Tier1sChannels = [ chA+'-'+chB for chA in Tier1s for chB in Tier1s ]

## translation between grid and lhcb naming conventions
transDict = { "CERNPROD" : "CERN", "FZK" : "GRIDKA",
              "INFNT1" : "CNAF", "FZKLCG2" : "GRIDKA",
              "IN2P3CC" : "IN2P3", "RALLCG2" : "RAL",
              "SARAMATRIX" : "SARA" }

def translate( ch ):
  """Translate channel name from grid to lhcb world.

  :param str ch: channel name
  """
  source, target = ch.split("-")
  if source in transDict: source = transDict[source]
  if target in transDict: target = transDict[target]
  return "%s-%s" % (source, target)

def extract( stdout , vo ):
  """Extract information about particular channel directly from command output.
  
  :param str stdout: stdout from glite-transfer-channel-list command
  """  
  name = origName = state = band = files = streams = ""
  voshare = 0.0
  allshares = 0.0
  for line in stdout.split("\n"):
    if line.startswith("Channel:"):
      origName = line.split(":")[1].strip()
      name = translate( origName )
    elif line.startswith("State:"):
      state = line.split(":")[1].strip()
    elif line.startswith("Bandwidth:"):
      band = line.split(":")[1].strip()
    elif line.startswith("VO '%s'" % vo ):
      voshare = int( line.split(":")[1].strip() )
      allshares += voshare
    elif "share is:" in line:
      allshares += int( line.split(":")[1].strip() )
    elif line.startswith("Number of files"):
      words = line.split()
      files = words[3][:-1]
      streams = words[5]
  share = "%d-%d" % ( voshare, allshares ) 
  return name, origName, files, streams, share, state, band
  
def execute():
  """main worker here, parses command line, executes a bunch of glite commands and prints the output 
  """
  usageStr = __doc__ + """
Usage:
	%s [option|cfgfile] 
""" % Script.scriptName + outputInfo
  Script.setUsageMessage( usageStr )
  Script.parseCommandLine()

  import DIRAC

  res = DIRAC.gConfig.getOption( "/DIRAC/VirtualOrganization" )
  if not res["OK"]:
    return res["Message"]
  vo = res["Value"]
  
  res = DIRAC.gConfig.getOptionsDict( "/Resources/FTSEndpoints")
  if not res["OK"]:
    return res["Message"] 
  URL2FTS = dict( [ (k, v.replace("FileTransfer", "ChannelManagement") ) for (k, v) in res["Value"].items() ]  )
  
  share = "Share %s-all" % vo
  fmtHead = " %-18s %-18s %-5s  %-7s  %-" + str(len(share)) + "s  %-10s  %-9s"
  fmt     = " %-18s %-18s %5s  %7s  %" + str(len(share)) + "s  %-10s  %9s"
  head = fmtHead % ( "Name", "OrigName", "Files", "Streams", share, "State", "Bandwidth" )
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
      if "VO '%s'" % vo in stdout:
        print fmt % extract( stdout, vo ) 

## entry point
if __name__ == "__main__":
  sys.exit( execute() )
