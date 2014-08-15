# import os
# import re
# import diracPilotLast
#
#
# diracScriptsPath = ''
# diracEnv=''
# rootPath=''
# SetupProjectRelease=''
#
# def main():
#   global diracEnv
#   lhcbdirac = LHCbPilot()
#   lhcbdirac.LHCbexecute()
#   if diracEnv and lhcbdirac.Configure():
#     lhcbdirac.StartJobAgent()
#   else:
#     diracPilotLast.pythonpathCheck()
#     install = diracPilotLast.InstallDIRAC()
#     install.setInstallOpt()
#     install.execute()
#     diracPilotLast.rootPath=os.getcwd()
#     diracScript = os.path.join( diracPilotLast.rootPath, 'scripts' )
#     if not diracEnv:
#       diracEnv={'Cert':'True'}
#     configure = diracPilotLast.ConfigureDIRAC(diracScript, diracPilotLast.rootPath, diracEnv, False)
#     configure.setConfigureOpt()
#     configure.execute()
#     configure.setInProcessOpts()
#     configure.startJobAgent()
#
# class LHCbPilot( object ):
#
#   def StartJobAgent(self):
#     global diracEnv
#     configure = diracPilotLast.ConfigureDIRAC(diracScriptsPath, rootPath, diracEnv, True)
#     configure.setInProcessOpts()
#     configure.startJobAgent()
#
#   def Configure(self):
#     global diracEnv
#     global rootPath
#     global SetupProjectRelease
#     __retCode__, diracScriptsPath = diracPilotLast.executeAndGetOutput( 'which dirac-configure', diracEnv )
#     #diracScriptsPath = os.path.dirname(diracScriptsPath)
#     diracScriptsPath=diracScriptsPath.replace("/dirac-configure","/")
#     rootPath=diracScriptsPath.replace("/InstallArea/scripts/","/")
#     diracPilotLast.logINFO ( 'Using the DIRAC installation in %s' % rootPath )
#     configure = diracPilotLast.ConfigureDIRAC(diracScriptsPath, rootPath, diracEnv,True)
#     configure.setConfigureOpt()
#     release = configure.releaseVersionList.split(',')
#     if SetupProjectRelease in release:
#       diracPilotLast.logINFO ( 'The release version %s is available in cvmfs' % SetupProjectRelease )
#       configure.releaseVersion=SetupProjectRelease
#       configure.execute()
#       return True
#     else:
#       diracPilotLast.logINFO ( 'The release version %s is NOT available cvmfs: start DIRAC Installation' % release )
#       return False
#
#
#
#
#   def LHCbexecute( self ):
#     global SetupProjectRelease
#     global diracEnv
#     try:
#       __retCode__, diracEnv, SetupProjectRelease = self.doSetupProject( '. /cvmfs/lhcb.cern.ch/lib/LbLogin.sh && . SetupProject.sh LHCbDirac && printenv ' )
#       print diracEnv
#       if diracEnv:
#         diracPilotLast.logINFO( "SetupProject DONE, the current release version is: %s" % SetupProjectRelease )
#       else:
#         diracPilotLast.logINFO( "SetupProject NOT DONE: start traditional DIRAC installation" )
#     except Exception:
#       diracPilotLast.logERROR ( "SetupProject NOT FOUND" )
#
#
#   def doSetupProject( self, cmd ):  # execute SetupProject
#     diracPilotLast.logDEBUG ( "Executing SetupProject" )
#     try:
#       import subprocess
#       _p = subprocess.Popen( "%s" % cmd, shell = True, stdout = subprocess.PIPE,
#                           stderr = subprocess.PIPE, close_fds = False )
#       ###################################################################################3
#       # Creating the dictionary containing the LHCbDirac environment
#       outData = {}
#       first=True
#       for line in _p.stdout:
#         if line.find('LHCbDirac')!=-1 and first:
#           SetupProjectRelease = re.findall(r'(?<=LHCbDirac)(.*?)(?=ready)',line)
#           SetupProjectRelease = SetupProjectRelease[0]
#           SetupProjectRelease = SetupProjectRelease.strip()
#           first=False
#         if line.find("=")!=-1:
#           line=line.strip()
#           parts=line.split('=')
#           outData[parts[0]]=parts[1]
#       returnCode = _p.wait()
#       return ( returnCode, outData, SetupProjectRelease )
#     except ImportError:
#       diracPilotLast.logERROR( "Error importing subprocess" )
#
#
#
#
# if __name__ == "__main__":
#     main()
