#!/usr/bin/env lb-run
# args: -r /cvmfs/lhcb.cern.ch/lib -c best ROOT/6.16.00 python

""" import ROOT can only be done in a separate environment, lb-run assure that (see 2 lines above)
    The version of ROOT used here (see "args" above) is "simply the latest" as of 03/2019

    This file has to be executable.
"""

import sys


def getRootFileGUID(fileName):
  """Function to retrieve a file GUID using Root
  """
  try:
    import ROOT
  except ImportError:
    exit("ROOT environment not set up")

  from ctypes import create_string_buffer
  try:
    ROOT.gErrorIgnoreLevel = 2001
    fr = ROOT.TFile.Open(fileName)
    branch = fr.Get('Refs').GetBranch('Params')
    text = create_string_buffer(100)
    branch.SetAddress(text)
    for i in xrange(branch.GetEntries()):
      branch.GetEvent(i)
      x = text.value
      if x.startswith('FID='):
        return x.split('=')[1]
    return exit('GUID not found')
  except Exception:
    return exit("Error extracting GUID")
  finally:
    if fr:
      fr.Close()


if __name__ == '__main__':
  sys.stdout.write(getRootFileGUID(sys.argv[1]))
