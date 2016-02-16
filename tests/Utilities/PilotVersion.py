import sys
from distutils.version import LooseVersion

setup = sys.argv[ 1 ]
    
from DIRAC.Core.Base.Script import parseCommandLine
parseCommandLine()

from DIRAC.ConfigurationSystem.Client.Helpers.Operations import Operations
pilotVersions = Operations( setup = setup ).getValue( 'Pilot/Version', [] )

pilotVersions = [ LooseVersion( pV ) for pV in pilotVersions ]
pilotVersions.sort()
    
print pilotVersions.pop().vstring

sys.exit( 0 )

#...............................................................................
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF