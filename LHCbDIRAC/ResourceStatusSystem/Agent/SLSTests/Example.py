# $HeadURL: $
'''  Example

  Module used as example and for tests.

'''

__RCSID__  = '$Id: $'
   
def getProbeElements():  
  return { 'OK' : True, 'Value' : [ 1, 2, 3 ] }

def setupProbes( testConfig ):
  return { 'OK' : True, 'Value' : testConfig }

def runProbe( probeInfo, testConfig ):
  return { 'xmlDict' : {}, 'config' : testConfig }

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF