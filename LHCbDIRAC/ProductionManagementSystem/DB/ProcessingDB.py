########################################################################
# $Id: ProcessingDB.py,v 1.1 2008/01/16 22:51:07 atsareg Exp $
########################################################################
""" DIRAC Processing DB

    Production database is used to collect and serve the necessary information
    in order to automate the task of job preparation for high level transformations.
"""

__RCSID__ = "$Id: ProcessingDB.py,v 1.1 2008/01/16 22:51:07 atsareg Exp $"

import re,time,types

from DIRAC import gConfig, gLogger, S_OK, S_ERROR
from DIRAC.Core.Base.DB import DB
from DIRAC.DataManagementSystem.Catalog.LcgFileCatalogClient import LcgFileCatalogClient

DEBUG = 1

#############################################################################

class ProcessingDB(DB):

  def __init__(self, maxQueueSize=10 ):

    DB.__init__(self,'ProcessingDB','ProductionManagement/ProcessingDB',maxQueueSize)

    self.filters = self.__getFilters()
    self.catalog = None

  def __getLFCClient(self):
    """Gets the LFC client instance
    """

    try:
      self.catalog = LcgFileCatalogClient()
      self.catalog.setAuthenticationID('/O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Andrei Tsaregorodtsev')
      result = S_OK()
    except Exception,x:
      self.log.exception("Failed to create LcgFileCatalogClient")
      result = S_ERROR(str(x))

    return result

  def addTransformation(self,prodID,inputDict):
    """Add transformation definition

       Add new transformation definition including its input streams
    """

    req = "INSERT INTO Transformations ( Production, CreateDate ) VALUES ( '" + \
          prodID+"', NOW() )"
    result = self._update(req)
    if not result['OK']:
      return result
    req = " SELECT LAST_INSERT_ID()"
    result = self._query(req)
    if result['OK']:
      transID = int(result['Value'])
    else:
      return result

    self.log.info( "ProcessingDB: New TransID %d" % transID )
    # Create corresponding input streams
    for inp in inputDict:
      name = inp['Name']
      mask = inp['LFNMask']
      if inp.has_key('GroupSize'):
        gsize = inp['GroupSize']
      else:
        gsize = 1
      req = "INSERT INTO InputStreams ( TransID, StreamName, LFNMask, GroupSize )" + \
            " VALUES ( "+transID+", '"+name+"', '"+mask+"', "+str(gsize)+" )"
      result = self._update(req)
      if not result['OK']:
        return result
      self.filters.append((transID,name,re.compile(mask)))

    result = self.__addProcessingTable(transID)
    # Add already existing files to this transformation if any
    result = self.__addExistingFiles(transID)
    return S_OK(transID)

  def __addExistingFiles(self,transID):
    """Add existing files to the transformation

       Add files that already exist in the DataFiles table to the
       transformation specified by the transID
    """

    # Add already existing files to this transformation if any
    filters = self.__getFilters(transID)
    req = "SELECT LFN,FileID FROM DataFiles"
    result = self._query(req)
    if result['OK']:
      files = result['Value']
      for lfn,fileID in files:
        resultFilter = self.__filterFile(lfn,filters)
        if resultFilter:
          result = self.__addFileToTransformation(fileID,resultFilter)
    else:
      return result

    return S_OK()

  def __addProcessingTable(self,transID):
    """Add Processing table

       Add a new Processing table for a given transformation
    """

    req = "CREATE TABLE P_"+str(transID) + " (" + """
FileID INTEGER NOT NULL,
StreamName VARCHAR(32),
Status VARCHAR(32) DEFAULT "unused",
ErrorCount INT(4) NOT NULL DEFAULT 0,
JobID CHAR(17),
UsedSE VARCHAR(32) DEFAULT "Unknown",
PRIMARY KEY (FileID,StreamName)
)"""
    result = self._update(req)
    if not result['OK']:
      return S_ERROR("ProcessingDB: failed to add new processing table "+str(x))
    else:
      return S_OK()

  def removeTransformation(self,production):
    """Remove transformation

       Remove the transformation specified by transID
    """

    res = self.getTransformation(production)
    transID = res['Transformation']['TransID']
    req = "DELETE FROM Transformations WHERE Production='"+str(production)+"'"
    result = self._update(req)
    if not result['OK']:
      return result
    req = "DELETE FROM InputStreams WHERE TransID="+str(transID)
    if not result['OK']:
      return result
    req = "DROP TABLE IF EXISTS P_"+str(transID)
    result = self._update(req)
    if not result['OK']:
      return result

    newfilters = []
    for tID,name,refilter in self.filters:
      if tID != transID:
        newfilters.append((tID,name,refilter))
    self.filters = newfilters

    return S_OK()

  def setTransformationStatus(self,transID,status):
    """Set the status of a transformation

       Set the status of the transformation specified by transID
    """
    req = "UPDATE Transformations SET Status='"+status+"' WHERE TransID="+transID
    result = self._update(req)
    if not result['OK']:
      return result
    return S_OK()

  def setTransformationProgress(self,transID,progress):
    """Set the progress of a transformation

       Set the progress of the transformation specified by transID
    """

    req = "UPDATE Transformations SET Progress="+str(progress)+" WHERE TransID="+str(transID)
    result = self._update(req)
    if not result['OK']:
      return result
    return S_OK()

  def getName(self):

    return "ProcessingDB"

  def exists(self,lfn):
    """Check file presence

       Check the presenc of the lfn in the Processing DB DataFiles table
    """
    req = "SELECT FileID FROM DataFiles WHERE LFN='"+lfn+"'"
    resQ = self._query(req)
    if resQ['OK']:
      result = S_OK()
      if len(resQ['Value']) == 0:
        result['Exists'] = 0
      else:
        result['Exists'] = 1
      return result
    else:
      return S_ERROR("ProcessingDB: failed to check existence of file "+lfn)

  def addFile(self,lfn,pfn,size,se,guid,force=False):
    """Add file

       Add a new file to the Production DB together with its first replica.
    """

    print "Adding file",lfn,pfn,size,se,guid

    pass_filter = 0
    retained = 0
    forced = 0
    added = 0
    added_to_transformation = 0
    lfn_exist = 0
    replica_exist = 0

    resultFilter = self.__filterFile(lfn)
    if resultFilter:
      pass_filter = 1
    if not resultFilter:
      if force:
        resultFilter = 1
        forced = 1

    if resultFilter:

      retained = 1
      result = self.__add_file(lfn,pfn,size,se,guid)
      lfn_exist = result['LfnExist']
      replica_exist = result['ReplicaExist']
      if result['Status'] == "OK":
        added = 1
        if resultFilter != 1:
          fileID = result['FileID']
          result = self.__addFileToTransformation(fileID,resultFilter)
          if result['Status'] == "OK":
            added_to_transformation = 1

    else:
      print "File",lfn,"is not requested"
      result = S_OK()
      result['Message'] = "File is not requested"

    result['PassFilter'] = pass_filter
    result['Retained'] = retained
    result['Forced'] = forced
    result['Added'] = added
    result['AddedToTransformation'] = added_to_transformation
    result['LfnExist'] = lfn_exist
    result['ReplicaExist'] = replica_exist

    return result

  def __add_file(self,lfn,pfn,size,se,guid):

    """Add file without checking for filters
    """

    #print "__add_file_without_filter: Adding file",lfn,pfn,size,se,guid

    lfn_exist = 0
    replica_exist = 0

    # Check if the file already added
    req = "SELECT FileID FROM DataFiles WHERE LFN='"+lfn+"'"
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']) == 0:
      req = "INSERT INTO DataFiles ( LFN, Status ) VALUES ( '"+lfn+"', 'New' )"
      result = self._update(req)
      if not result['OK']:
        return result
      req = " SELECT LAST_INSERT_ID()"
      if not result['OK']:
        return result
      fileID = result['Value'][0][0]
    else:
      fileID = str(int(result[0][0]))
      lfn_exist = 1

    req = "SELECT FileID FROM Replicas WHERE FileID="+fileID+" AND SE='"+se+"'"
    result = self._query(req)
    if not result['OK']:
      return result
    if len(result['Value']):
      req = "INSERT INTO Replicas ( FileID, SE, PFN ) VALUES ( "+fileID+", '"+se+"', '"+pfn+"' )"
      result = self._update(req)
      if not result['OK']:
        return result
    else:
      replica_exist = 1

    result = S_OK()
    result['FileID'] = fileID
    result['LfnExist'] = lfn_exist
    result['ReplicaExist'] = replica_exist

    return result

  def addFiles(self,lfns,force=False):
    """Add files

       Add a list of replicas in one go
    """

    ok = 1
    counter = 0
    counter_not_retained = 0
    counter_add = 0
    counter_bad = 0
    counter_force = 0
    counter_exist = 0

    for se,lfn in lfns:

      counter += 1
      pfn = se+":"+lfn
      result = self.addFile(lfn,pfn,999999,se,'0000000000000',force)
      if result['Status'] != 'OK':
        print "Failed to add file",lfn,"at",se
        ok = 0
        counter_bad += 1
      else:
        if result.has_key('Message') and result['Message'] == 'File is not requested':
          counter_not_retained += 1
        if result['Forced'] :
          counter_force += 1
        if result['ReplicaExist'] :
          counter_exist += 1
        if result['Added']:
          counter_add += 1
          print counter_add,"Added file",lfn,"at",se

    result = S_OK()

    rstring = "Received "+str(counter)+" replicas: added "+ \
                  str(counter-counter_not_retained-counter_exist)+ \
            ", not retained "+ str(counter_not_retained)+ \
                  ", forced "+ str(counter_force)+", already existed "+str(counter_exist)+\
                  ", failed "+str(counter_bad)
    result['Message'] = rstring
    result['Added'] = counter_add
    result['Forced'] = counter_force
    result['Bad'] = counter_bad
    result['ReplicaExist'] = counter_exist

    return result


  def addPfn(self,lfn,pfn,se):
    """Add replica

       Add new replica to the Production DB for an existing lfn.
       This is just for compatibility with the File Catalog interface.
    """

    result = self.addFile(lfn,pfn,999999,se,'01234567890')
    return result

  def addPfns(self,listOfTuples):
    """Add a list of replicas (pfn) to the catalog """



    result = S_OK()
    listOfMessages = []
    listOfStatus = []
    result['Message'] = "There is a message per added file in result['listOfMessages']"



    for lfn,pfn,se in listOfTuples:
      print 'addPfns: lfn,pfn,se in listOfTuples :'
      print lfn,pfn,se
      res = self.addPfn(lfn,pfn,se)
      if ( res['Status'] == 'OK' ):
         listOfMessages.append("File " + pfn + " inserted in the catalog")
         listOfStatus.append((lfn,pfn,'Done'))
      else:
         listOfMessages.append("Failed to insert file " + pfn + "\n" + res['Message'])
         listOfStatus.append((lfn,pfn,'Failed'))
         result['Status']= "Error"



    result['listOfStatus'] = listOfStatus
    result['listOfMessages'] = listOfMessages
    return result

  def addDirectory(self,directory,force=False):
    """Add a directory

       Adds all the files stored in a given directory in the LFC catalog.
       Adds all the replicas of the processed files
    """

    print "Adding files in the directory",directory

    if self.catalog is None:
      result = self.__getLFCClient()

    if self.catalog is None:
      print "Failed to get the LFC client"
      return S_ERROR("Failed to get the LFC client")

    start = time.time()
    result = self.catalog.getPfnsInDir(directory)
    end = time.time()
    print "getPfnsInDir",directory,"operation time",(end-start)
    if result['Status'] == 'OK':
      lfndict =  result['Replicas']
      counter_lfn = 0
      counter = 0
      lfns = []
      for lfn,repdict in lfndict.items():
        counter_lfn += 1
        for se,pfn in repdict.items():
          counter += 1
          lfns.append((se,lfn))

      result = self.addFiles(lfns,force)
      added = result['Added']
      forced = result['Forced']

      rstring = "Lookup "+str(counter_lfn)+" lfns, added "+ \
                str(added)+ " replicas, forced"+ str(forced)+" replicas"
      print rstring
      return S_OK(rstring)
    else:
      print "Failed to ls directory",directory
      return S_ERROR("Failed to ls directory "+directory)

  def getPfnsByLfn(self,lfn,session=False):
    """Get replicas

       Get replicas for the file specified by lfn
    """

    repdict = {}
    req = "SELECT FileID from DataFiles WHERE LFN='"+ lfn+"'"
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      print "ProcessingDB: LFN not found",lfn
      return S_ERROR("ProcessingDB: LFN not found "+lfn)

    fileID = result['Value'][0][0]
    req = "SELECT SE,PFN FROM Replicas WHERE FileID="+str(fileID)
    result = self._query(req)
    if not result['OK']:
      return result

    if result['Value']:
      for row in result['Value']:
        repdict[row[0]] = row[1]
    result = S_OK()
    result['Replicas'] = repdict
    return result

  def getPfnsByLfnList(self,lfns):
    """Get replicas for the files specified by the lfn list
    """

    resdict = {}
    result = S_OK()

    for lfn in lfns:
      result = self.getPfnsByLfn(lfn,False)
      if result['Status'] == 'OK':
        resdict[lfn] = result['Replicas']
      else:
        resdict[lfn] = {}

    result['Replicas'] = resdict
    return result

  def removePfn(self,lfn,pfn):
    """Remove replica

       Remove replica pfn of lfn
    """

    req = "DELETE Replicas FROM Replicas,DataFiles WHERE "+ \
          "Replicas.FileID=DataFiles.FileID AND DataFiles.LFN='"+ \
          lfn+"' AND Replicas.PFN='"+pfn+"'"
    result = self._update(req)
    return result

  def rmFile(self,lfn,update_transformations=True):
    """Remove file

       Remove file specified by lfn from the ProcessingDB
    """

    req = "DELETE Replicas FROM Replicas,DataFiles WHERE Replicas.FileID=DataFiles.FileID AND DataFiles.LFN='"+lfn+"'"
    result = self._update(req)
    req = "DELETE FROM DataFiles WHERE LFN='"+lfn+"'"
    result = self._update(req)
    result = self.getAllTransformations()
    if result['Status'] == "OK":
      for t in result["Transformations"]:
        transID = t['TransID']
        print "++++",transID,'any','deleted',lfn
        result = self.setFileStatusForTransformation(transID,'any','deleted',[lfn])
    print "File",lfn,"removed from the Processing DB"
    return S_OK()

  def __getFilters(self,transID=None):
    """Get filters

       Get filters for all defined input streams in all the transformations.
       If transID argument is given, get filters only for this transformation.
    """

    resultList = []
    req = "SELECT TransID,StreamName,LFNMask FROM InputStreams"
    result = self._query(req)
    if not result['OK']:
      return result
    for transID,name,mask in result['Value']:
      refilter = re.compile(mask)
      resultList.append((transID,name,refilter))

    return resultList

  def __filterFile(self,lfn,filters=None):
    """Pass the input file through a filter

       Apply input file filters of the currently active transformations to the
       given lfn and select appropriate transformations if any. If 'filters'
       argument is given, use this one instead of the global filter list.
       Filter list is composed of triplet tuples transID,StreamName,refilter
       where refilter is a compiled RE object to check the lfn against.
    """

    result = []

    # If the list of filters is given use it, otherwise use the complete list
    if filters:
      for transID,name,refilter in filters:
        #print transID,name,refilter
        if refilter.search(lfn):
          result.append((transID,name))
    else:
      for transID,name,refilter in self.filters:
        #print transID,name,refilter
        if refilter.search(lfn):
          result.append((transID,name))

    #print result
    return result

  def __addFileToTransformation(self,fileID,resultFilter):
    """Add file to transformations

       Add file to all the transformations which require this kind of files.
       resultFilter is a list of pairs transID,StreamName which needs this file
    """

    if resultFilter:
      for transID,name in resultFilter:
        req = "SELECT * FROM P_"+str(transID)+" WHERE FileID="+str(fileID)+ \
              " AND StreamName='"+name+"'"
        result = self._query(req)
        if not result['OK']:
          return result
        if result['Value']:
          req = "INSERT INTO P_"+str(transID)+" ( FileID, StreamName ) VALUES ( " + \
                str(fileID)+", '"+name+"' )"
          result = self._update(req)
          if not result['OK']:
            return result
        else:
          print "File",fileID,name,"already added to transformation",transID

    return S_OK()

  def getTransformationStats(self,production):
    """Get Transformation statistics

       Get the statistics of Transformation idendified by production ID
    """

    result = self.getTransformation(production)
    if result['Status'] == "OK":
      transID = result['Transformation']['TransID']
      req = "SELECT COUNT(*) FROM P_"+str(transID)
      result = self.query(req)
      if not result['OK']:
        return result
      total = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM P_"+str(transID)+" WHERE Status='unused'"
      result = self.query(req)
      if not result['OK']:
        return result
      unused = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM P_"+str(transID)+" WHERE Status='assigned'"
      result = self.query(req)
      if not result['OK']:
        return result
      assigned = int(result['Value'][0][0])
      req = "SELECT COUNT(*) FROM P_"+str(transID)+" WHERE Status='done'"
      result = self.query(req)
      if not result['OK']:
        return result
      done = int(result['Value'][0][0])

      stats = {}
      stats['Total'] = total
      stats['Unused'] = unused
      stats['Assigned'] = assigned
      stats['Done'] = done
      result = S_OK()
      result['Stats'] = stats
      return result

    else:
      print "ProcessingDB: unknown transformation",production
      return S_ERROR("ProcessingDB: unknown transformation")

  def getTransformation(self,production):
    """Get Transformation definition

       Get the parameters of Transformation idendified by production ID
    """

    result = S_OK()
    transdict = {}
    req = "SELECT TransID,Status,Progress FROM Transformations "+\
          "WHERE Production='"+str(production)+"'"
    result = self.query(req)
    if not result['OK']:
      return result
    if result['Value']:
      row = result['Value'][0]
      transdict['TransID'] = row[0]
      transdict['Status'] = row[1]
      transdict['Progress'] = float(row[2])
      result["Transformation"] = transdict
      return result
    else:
      return S_ERROR('Transformation not found')

  def modifyTransformationInput(self,production,lfnmask,stream="any"):
    """Modify input stream

       Modify the input stream definition for the given transformation
       identified by production
    """

    result = self.getTransformation(production)
    transID = result["Transformation"]['TransID']
    result = self.getInputStreams(transID)
    streams = result['InputStreams']
    for s in streams:
      if stream != "any":
        if s != stream:
          continue
        else:
          req = "UPDATE InputStreams SET LFNMask='"+lfnmask+ \
                  "' where TransID="+str(transID)+" AND StreamName='"+stream
      else:
        req = "UPDATE InputStreams SET LFNMask='"+lfnmask+ \
                  "' where TransID="+str(transID)
      result = self._update(req)
      if not result['OK']:
        return result

    return S_OK()

  def changeTransformationProduction(self,transID,new_production):
    """ Change the production ID assiciated with the given transformation
    """

    req = "UPDATE Transformations SET Production='"+new_production+ \
          "' where TransID="+str(transID)
    result = self._update(req)
    if not result['OK']:
      return result

    result = S_OK()
    return result

  def getAllTransformations(self):
    """Get Transformation information

       Get parameters of all the Transformations
    """

    result = S_OK()
    translist = []
    req = "SELECT TransID,Production,Status,Progress FROM Transformations "
    print req
    resQ = self._query(req)
    if not resQ['OK']:
      return resQ
    for row in resQ['Value']:
      transdict = {}
      transdict['TransID'] = row[0]
      transdict['Production'] = row[1]
      transdict['Status'] = row[2]
      transdict['Progress'] = float(row[3])
      translist.append(transdict)

    result["Transformations"] = translist
    return result

  def getInputStreams(self,transID):
    """ Get input stream names for the given transformation
    """

    result = S_OK()
    streams = {}
    req = "SELECT StreamName,LFNMask,GroupBySiteFlag,GroupSize FROM InputStreams WHERE TransID="+str(transID)
    resQ = self._query(req)
    if not resQ['OK']:
      return resQ
    for row in resQ['Value']:
      streams[row[0]] = (row[1],row[2],int(row[3]))
    result["InputStreams"] = streams
    return result

  def getFilesForTransformation(self,production,order_by_job=False):
    """ Get files and their status for the given transformation
    """

    res = self.getTransformation(production)
    if res['Status'] != "OK":
      return S_ERROR("Transformation is not found")
    transID = res['Transformation']['TransID']

    flist = []
    req = "SELECT LFN,p.Status,StreamName,p.JobID,p.UsedSE FROM DataFiles AS d,P_"+ \
          str(transID)+" AS p WHERE "+"p.FileID=d.FileID ORDER by LFN"
    if order_by_job:
      req = "SELECT LFN,p.Status,StreamName,p.JobID,p.UsedSE FROM DataFiles AS d,P_"+ \
            str(transID)+" AS p WHERE "+"p.FileID=d.FileID ORDER by p.JobID"

    result = self._query(req)
    if not result['OK']:
      return result
    for lfn,status,stream,jobid,usedse in dbc.fetchall():
      print lfn,status,stream,jobid,usedse
      fdict = {}
      fdict['LFN'] = lfn
      fdict['Status'] = status
      fdict['Stream'] = stream
      if jobid is None: jobid = 'No JobID assigned'
      fdict['JobID'] = jobid
      fdict['UsedSE'] = usedse
      flist.append(fdict)

    result = S_OK()
    result['Files'] = flist
    print result
    return result

  def getTransformationDefinition(self,production):
    """ Gets the definition of the transformation including its input stream
    """

    result = self.getTransformation(production)
    if result['Status'] == "OK":
      transID = result['Transformation']['TransID']
      resultIS = self.getInputStreams(transID)
      #print resultIS
      if resultIS['Status'] == "OK":
        result = S_OK()
        result['InputStreams'] = resultIS['InputStreams']
        return result
      else:
        return resultIS
    else:
      return result


  def getInputDataForStream(self,transID,stream,status):
    """ Get input data for the given transformation/input streams, only files
        with a given status which is defined for the file replicas.
    """

    reslist = []
    req = "SELECT FileID from P_"+str(transID)+" WHERE Status='unused'"
    result = self._query(req)
    if not result['OK']:
      return result

    if result['Value']:
      ids = [ str(x[0]) for x in result['Value'] ]
    if not ids:
      result = S_OK()
      result['Data'] = []
      return result

    fileids = string.join(ids,",")
    req = "SELECT LFN,SE FROM Replicas,DataFiles WHERE Replicas.FileID=DataFiles.FileID and "+ \
          "Replicas.FileID in ("+fileids+")"
    result = self._query(req)
    if not result['OK']:
      return result
    for lfn,se in result['Value']:
      reslist.append((lfn,se))

    result = S_OK()
    result['Data'] = reslist
    return result

  def __getFileIDsForLfns(self,lfns):
    """ Get file Ids for the given list of lfn's
    """

    fids = []

    str_lfns = []
    for lfn in lfns:
      str_lfns.append("'"+lfn+"'")
    str_lfn = string.join(str_lfns,",")

    req = "SELECT FileID FROM DataFiles WHERE LFN in ( "+str_lfn+" )"
    result = self._query(req)
    if not result['OK']:
      return result
    for row in result['Value']:
      fids.append(str(row[0]))

    return fids

  def setFileSEForTransformation(self,transID,se,lfns):
    """ Set file SE for the given transformation identified by transID
        for files in the list of lfns
    """

    fids = self.__getFileIDsForLfns(lfns)

    if fids:
      s_fids = string.join(fids,",")
      req = "UPDATE P_"+str(transID)+" SET FileSE='"+se+"' WHERE FileID IN ( "+ \
            s_fids+" ) "
      print req
      result = self._update(req)
      return result

    return S_ERROR('Files not found')

  def setReplicaStatus(self,lfn,status,site):
    """Set file status

       Set file status for the replica specified by se
    """

    fids = self.__getFileIDsForLfns([lfn])
    if fids:
      fileID = fids[0]
    else:
      print "File",lfn,"not known"
      return S_ERROR("File "+lfn+" not known")

    if site.lower() == "any" :
      req = "UPDATE Replicas SET Status='"+status+"' WHERE FileID="+fileID
      result = self._update(req)
    else:
      result = gConfig.getValue('/Resources/SiteLocalSEMapping',site,types.ListType)
      if result['Status'] == 'OK':
        ses = [ "'"+x+"'" for x in result['Value'] ]
        sesstr = string.join(ses,',')
        req = "UPDATE Replicas SET Status='"+status+"' WHERE FileID="+fileID+ \
              " AND SE IN ("+sesstr+")"
        result = self._update(req)
      else:
        return S_ERROR('Failed to get SiteLocalSEMapping')

    if status == "Problematic":
      req = "UPDATE DataFiles SET Status='Problematic' WHERE FileID="+fileID
      result = self._update(req)

    return S_OK()

  def getReplicaStatus(self,lfn):
    """ Get all the replica information for a given LFN
    """

    print "Getting replica status for",lfn

    repdict = {}
    req = "SELECT FileID from DataFiles WHERE LFN='"+ lfn+"'"
    result = self._query(req)
    if not result['OK']:
      return result
    if not result['Value']:
      print "ProcessingDB: LFN not found",lfn
      return S_ERROR("ProcessingDB: LFN not found "+lfn)

    fileid = int(result['Value'][0][0])
    req = "SELECT SE,PFN,Status FROM Replicas WHERE FileID="+str(fileid)
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value']:
      for row in result['Value']:
        pfn = row[1]
        status = row[2]
        repdict[row[0]] = (pfn,status)
    result = S_OK()
    result['ReplicaStatus'] = repdict
    return result

  def setFileStatusForTransformation(self,transID,stream,status,lfns):
    """ Set file status for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """

    fids = self.__getFileIDsForLfns(lfns)

    if fids:
      s_fids = string.join(fids,",")
      if stream.lower() == "any":
        req = "UPDATE P_"+str(transID)+" SET Status='"+status+"' WHERE FileID IN ( "+ \
              s_fids+" )"
      else:
        req = "UPDATE P_"+str(transID)+" SET Status='"+status+"' WHERE FileID IN ( "+ \
              s_fids+" ) AND StreamName='"+stream+"'"
      print req
      result = self._update(req)
      return result

  def setFileStatus(self,production,lfn,status):
    """ Set file status for the given production identified by production
        for the given lfn
    """
    result = self.getTransformation(production)
    if result['Status'] == "OK":
      transID = result['Transformation']['TransID']
      result = self.setFileStatusForTransformation(transID,"any",status,[lfn])
      if result['Status'] != "OK":
        print "Failed to set status for file",lfn,"production",production
    else:
      print "Failed to set status for file",lfn,"production",production

    return result

  def setFileJobID(self,transID,stream,jobID,lfns):
    """ Set file job ID for the given transformation identified by transID
        for the given stream for files in the list of lfns
    """

    fids = self.__getFileIDsForLfns(lfns)

    if fids:
      s_fids = string.join(fids,",")
      if stream == "any":
        req = "UPDATE P_"+str(transID)+" SET JobID='"+jobID+"' WHERE FileID IN ( "+ \
              s_fids+" )"
      else:
        req = "UPDATE P_"+str(transID)+" SET JobID='"+jobID+"' WHERE FileID IN ( "+ \
              s_fids+" ) AND StreamName='"+stream+"'"

      result = self._update(req)
      return result

    else:
      return S_ERROR('Files not found')

  def getSiteBlackList(self):
    """ Get the list of sites which are not allowed to take jobs
    """

    site_list = []
    req = "SELECT Site FROM SiteBlackList"
    result = self._query(req)
    if not result['OK']:
      return result
    if result['Value']:
      for row in result['Value']:
          site_list.append(row[0])
    return S_OK(site_list)

  def addSiteToBlackList(self,site,comment='None'):
    """ Add a site name to the Site Black List
    """

    result = self.getSiteBlackList()
    if result['Status'] == "OK":
      site_list = result['Value']
      if site in site_list:
        return S_OK()

    req = "INSERT INTO SiteBlackList (Site, Comment) VALUES ('%s','%s')" % (site,comment)
    print req
    result = self._update(req)
    return result

  def removeSiteFromBlackList(self,site):
    """ Remove a site name form the Site Black List
    """

    req = "DELETE FROM SiteBlackList WHERE Site='"+site+"'"
    print req
    result = self._update(req)
    return result