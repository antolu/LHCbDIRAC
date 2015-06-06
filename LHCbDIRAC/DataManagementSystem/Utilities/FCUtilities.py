"""
   This modules contains utility functions for LHCb DM
"""
__RCSID__ = "$Id $"
from DIRAC import S_OK, S_ERROR, gLogger
import sys, os
from DIRAC.Resources.Catalog.FileCatalogClient import FileCatalogClient

def chown( directories, user = None, group = None, mode = None, recursive = False, ndirs = None, fcClient = None ):
  """
  This method may change the user, group or mode of a directory and apply it recursively if required
  """
  if ndirs is None:
    ndirs = 0
  if not directories:
    return S_OK( ndirs )
  if isinstance( directories, basestring ):
    directories = [directories]
  if fcClient is None:
    fcClient = FileCatalogClient()
  if user is not None:
    res = fcClient.changePathOwner( dict.fromkeys( directories, user ) )
    if not res['OK']:
      res = fcClient.changePathOwner( dict.fromkeys( directories, {'Owner' : user} ) )
      if not res['OK']:
        return res
  if group is not None:
    res = fcClient.changePathGroup( dict.fromkeys( directories, group ) )
    if not res['OK']:
      res = fcClient.changePathGroup( dict.fromkeys( directories, {'Group':group} ) )
      if not res['OK']:
        return res
  if mode is not None:
    res = fcClient.changePathMode( dict.fromkeys( directories, mode ) )
    if not res['OK']:
      res = fcClient.changePathMode( dict.fromkeys( directories, {'Mode':mode} ) )
      if not res['OK']:
        return res
  if recursive:
    for subDir in directories:
      if ndirs % 10 == 0:
        sys.stdout.write( '.' )
        sys.stdout.flush()
      ndirs += 1
      res = fcClient.listDirectory( subDir )
      if res['OK']:
        subDirectories = res['Value']['Successful'][subDir]['SubDirs']
        if subDirectories:
          # print subDir, len( subDirectories ), ndirs
          res = chown( subDirectories, user, group = group, mode = mode, recursive = True, ndirs = ndirs, fcClient = fcClient )
          if not res['OK']:
            return res
          ndirs = res['Value']
  else:
    ndirs += 1
  return S_OK( ndirs )

def createUserDirectory( user ):
  """
  This functions creates (if not existing) a user directory in the DFC
  """
  dfc = FileCatalogClient()
  initial = user[0]
  baseDir = os.path.join( '/lhcb', 'user', initial, user )
  if dfc.isDirectory( baseDir ).get( 'Value', {} ).get( 'Successful', {} ).get( baseDir ):
    return S_ERROR( 'User directory already existing' )
  gLogger.info( 'Creating directory', baseDir )
  res = dfc.createDirectory( baseDir )
  if not res['OK']:
    return res
  gLogger.info( 'Setting ownership of directory', baseDir )
  return chown( baseDir, user, group = 'lhcb_user', mode = 0755, recursive = False, fcClient = dfc )
