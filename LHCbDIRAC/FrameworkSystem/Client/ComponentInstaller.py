"""
Class for the management of non-standard DIRAC components: vcycle, squid
"""

import subprocess

from DIRAC import S_OK, S_ERROR
from DIRAC.FrameworkSystem.Client.ComponentInstaller import ComponentInstaller as DIRACComponentInstaller

class ComponentInstaller(DIRACComponentInstaller):

  def runsvctrlComponent( self, system, component, mode ):
    if component.lower() == 'vcycle' or component.lower() == 'squid':
      if mode == 'u':
        command = 'start'
      elif mode == 't':
        command = 'restart'
      elif mode == 'd':
        command = 'stop'

      if component.lower() == 'vcycle':
        command = '/etc/rc.d/init.d/vcycled ' + command
      elif component.lower() == 'squid':
        command = 'service squid ' + command

      try:
        result = subprocess.call( command, shell = True )
        if result == 0:
          return S_OK( '%s : Successful' % ( command ) )
        else:
          return S_ERROR( '%s : Failed (exit code: %s)' % ( command, result ) )
      except Exception as e:
        return S_ERROR( repr(e) )

    else:
      super(ComponentInstaller).runsvctrlComponent(system, component, mode)
