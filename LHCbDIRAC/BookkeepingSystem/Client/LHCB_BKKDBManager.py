########################################################################
# $Id: LHCB_BKKDBManager.py,v 1.2 2008/06/10 11:45:00 zmathe Exp $
########################################################################

"""
LHCb Bookkeeping database manager
"""

from DIRAC                                                               import gLogger, S_OK, S_ERROR
from DIRAC.BookkeepingSystem.Client.BaseESManager                        import BaseESManager
from DIRAC.BookkeepingSystem.Client.BookkeepingClient                    import BookkeepingClient
from DIRAC.BookkeepingSystem.Client.objects                              import Entity
import os
import types
import sys

__RCSID__ = "$Id: LHCB_BKKDBManager.py,v 1.2 2008/06/10 11:45:00 zmathe Exp $"

INTERNAL_PATH_SEPARATOR = "/"

############################################################################# 
class LHCB_BKKDBManager(BaseESManager):
    
  LHCB_BKDB_FOLDER_TYPE = "LHCB_BKDB_Folder"
  LHCB_BKDB_FILE_TYPE = "LHCB_BKDB_File"
    
                                       
  LHCB_BKDB_FOLDER_PROPERTIES = ['name', 
                                 'fullpath',
                                 'expandable' # normally true
                                        ]    
    # watch out for this ad hoc solution
    # if any changes made check all functions
    #
  LHCB_BKDB_PREFIXES =     ['CFG',    # configurations
                                 'EVT',    # event type
                                 '',    # filename                                 
                                 ]
    
  LHCB_BKDB_PREFIX_SEPARATOR = "_"
  
  ############################################################################# 
  def __init__(self):
    super(LHCB_BKKDBManager, self).__init__()
    self._BaseESManager___fileSeparator = INTERNAL_PATH_SEPARATOR    
    #self.__pathSeparator = INTERNAL_PATH_SEPARATOR
    self.db_ = BookkeepingClient()
    self.entityCache_ = {'/':(Entity({'name':'/', 'fullpath':'/', 'expandable':True, 'type':self.LHCB_BKDB_FOLDER_TYPE}), 0)}

  ############################################################################# 
  def list(self, path=""):
       
    entityList = list()
    path = self.getAbsolutePath(path)['Value'] # shall we do this here or in the _processedPath()?
    valid, processedPath = self._processPath(path)
    if not valid:
      gLogger.error(path + " is not valid!");
      raise ValueError, "Invalid path '%s'" % path
        # get directory content
    levels = len(processedPath)
        
        
    if levels == 0:    
      # list root
      gLogger.debug("listing configurations")
      dbResult = self.db_getAviableConfigNameAndVersion()
      for record in dbResult:
        entityList += [self._getEntityFromPath(path, record, levels)]
        self._cacheIt(entityList)
    
    return S_OK(entityList)                    
 
  ############################################################################# 
  def _getEntityFromPath(self, presentPath, newPathElement, level):
    
    if isinstance(newPathElement, types.DictType):
      # this must be a file
      entity = Entity(newPathElement)
      newPathElement = str(entity['name']).rsplit("/", 1)[1]
      entity.update({'gname':entity['name']})
      expandable = False
      type = self.LHCB_BKDB_FILE_TYPE                            
    else:
      # this must be a folder
      entity = Entity()
      newPathElement = self.LHCB_BKDB_PREFIXES[level]+ \
      self.LHCB_BKDB_PREFIX_SEPARATOR + \
      newPathElement
      expandable = True
      type = self.LHCB_BKDB_FOLDER_TYPE                            
                            
      fullPath  = presentPath.rstrip(INTERNAL_PATH_SEPARATOR)
      fullPath += INTERNAL_PATH_SEPARATOR + \
      newPathElement
      entity.update({'name':newPathElement, 'fullpath':fullPath, 'expandable':expandable, 'type':type})
    
    return entity
       
#    takes an absolute path and returns of tuples with prefixes and posfixes
#    of path elements. If invalid path returns null
  ############################################################################# 
  def _processPath(self, path):
    path = path.encode('ascii')
    correctPath = True
    path = path.strip(INTERNAL_PATH_SEPARATOR + " ")
    tokens = path.split(self.getPathSeparator())
    if tokens == ['']:
      # path is root i.e. '\'
      return True, []
    result = []
    counter = 0;
    fileNameDetected = False        
    for token in tokens:
      prefix, suffix = self._splitPathElement(token)
      if prefix == self.LHCB_BKDB_PREFIXES[11]:    # '' i.e. filename
        if counter not in [2]:            # any of the possible locations in the prefixes list
          correctPath = False
          break
        fileNameDetected = True                    # remember that the path should be closed
      else:
        if self.LHCB_BKDB_PREFIXES[counter] != prefix or fileNameDetected:
          # the prefix is not at the right location in the path or it is coming too late
          correctPath = False
          break                        

      result += [(prefix, suffix)]
      counter += 1
      gLogger.debug("processPath=" + str(result))    
    
    return (correctPath, result)
    
#   it caches an entity or a list of entities
  ############################################################################# 
  def _cacheIt(self, entityList):
    if isinstance(entityList, Entity):
      # convert it into a list
      entityList = [entityList]
    elif not isinstance(entityList, types.ListType):
      # neither entity nor list
      gLogger.warn("couldn't cache invalid entity(list) of type " + str(entityList.__class))
      return
        
    for entity in entityList:
      # TO IMPLEMENT!! time of the caching
      try:
        self.__entityCache.update({entity['fullpath']: (entity, 0)})
      except:
        gLogger.warn("couldn't cache entity(?) " + str(entity))
      return 
        
  #############################################################################       
  def getAbsolutePath(self, path):
    # get current working directory if empty
    if path in [ "", ".", None] :
      path = INTERNAL_PATH_SEPARATOR # root        
      # convert it into absolute path    
    path = os.path.normpath(path)
    if os.sep != INTERNAL_PATH_SEPARATOR:
      path = path.replace(os.sep, INTERNAL_PATH_SEPARATOR)
            
    # for this special case of anomaly when double // may appear
    path = path.replace(2*INTERNAL_PATH_SEPARATOR, INTERNAL_PATH_SEPARATOR)
    
    return S_OK(path)
  
  #############################################################################       
  def get(self, path = ""):
    path = self.getAbsolutePath(path)['Value']
    entity = self._getEntity(path)
    if entity.__class__ == types.NoneType:
      gLogger.error(path + " doesn't exist!");
      raise ValueError, "Invalid path %s" % path
    return S_OK(entity)
    
  #############################################################################       
  def _getEntity(self, path):
    #
    # This is not doing anything at the moment
    #
    #
        
    # First try
    try:
      entity = self.__entityCache[path][0]
      gLogger.debug("getting " + str(path) + " from the cache")
      return entity
    except:
      # not cached so far
      gLogger.debug(str(path) + " not in cache. Fetching...")
      entityList = self.list(self.mergePaths(path, ".."))
      self._cacheIt(entityList)
            

    # Second try
    try:
      gLogger.debug("getting " + str(path) + " eventually from the cache")
      entity = self.__entityCache[path][0]
      return entity
    except:
      # still not in the cache... wrong path
      gLogger.warning(str(path) + " seems to be a wrong path");
      return None
        
    return entity