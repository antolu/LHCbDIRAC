# $Id: ProductionRequestDB.py,v 1.1 2009/02/11 10:52:32 azhelezo Exp $
"""
    DIRAC ProductionRequestDB class is a front-end to the repository
    database containing Production Requests and other related tables.
"""
__RCSID__ = "$Revision: 1.1 $"

import string
from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR
from DIRAC.Core.Utilities import Time

import threading

class ProductionRequestDB(DB):
  def __init__( self, maxQueueSize=10 ):
    """ Constructor
    """
    DB.__init__(self,'ProductionRequestDB',
                'ProductionManagement/ProductionRequestDB', maxQueueSize)
    self.lock = threading.Lock()

#################### Production Requests table ########################
  requestFields = [ 'RequestID', 'ParentID', 'MasterID', 'RequestAuthor',
                    'RequestName', 'RequestType', 'RequestState',
                    'RequestPriority', 'RequestPDG',
                    'SimCondition', 'SimCondID', 'SimCondDetail',
                    'ProPath', 'ProID', 'ProDetail',
                    'EventType', 'NumberOfEvents', 'Description', 'Comments',
                    'HasSubrequest', 'bk', 'bkTotal', 'rqTotal' ] # runtime

  historyFields = [ 'RequestID', 'RequestState', 'RequestUser', 'TimeStamp' ]

# !!! current _escapeValues is buggy !!! None and not using connection...
# _insert use it, so I can't...
  def _fixedEscapeValues(self,inValues):
    result = self._escapeValues(inValues)
    if not result['OK']:
      return result
    outValues = result['Value']
    for i,x in enumerate(outValues):
      if x == 'None':
        outValues[i] = 'NULL'
    return S_OK(outValues)

  def __getRequestInfo(self,id,connection):
    """ Retrive info fields from specified ID
        Used to get ParentID information.
        id must be checked before
        NOTE: it does self.lock.release() in case of errors
    """
    inFields  = [ 'RequestState','ParentID','MasterID' ]
    result = self._query("SELECT %s " % ','.join(inFields) +
                         "FROM ProductionRequests " +
                         "WHERE RequestID=%s;" % id, connection)
    if not result['OK']:
      self.lock.release()
      return result
    if len(result['Value']) == 0:
      self.lock.release()
      return S_ERROR('Request does not exist')
    return S_OK(dict(zip(inFields,result['Value'][0])))

  def __getState(self,id,connection):
    """ Return state of Master for id (or id's own if no parents)
        id must be checked before
        NOTE: it does self.lock.release() in case of errors
    """
    result = self.__getRequestInfo(id,connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    if not pinfo['MasterID']:
      return S_OK(pinfo['RequestState'])
    result = self.__getRequestInfo(pinfo['MasterID'],connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    return S_OK(pinfo['RequestState'])

  def __checkMaster(self,master,id,connection):
    """ Return State of Master for id (or id's own if no parents)
        id and master must be checked before. It check that master can
        be reached with ParentID links.
        NOTE: it does self.lock.release() in case of errors
    """
    while True:
      result = self.__getRequestInfo(id,connection)
      if not result['OK']:
        return result
      pinfo = result['Value']
      if id == master:
        return S_OK(pinfo['RequestState'])
      if pinfo['MasterID'] != master:
        self.lock.release()
        return S_ERROR('Wrong MasterID for this ParentID')
      if not pinfo['ParentID'] or pinfo['ParentID'] == id:
        self.lock.release()
        return S_ERROR('Parent tree is broken. Please contact expert')
      id = pinfo['ParentID']

  def createProductionRequest(self, requestDict):
    """ Create new Production Request
        TODO: Protect fields in subrequests
    """
    rec = dict.fromkeys(self.requestFields[1:-4],None)
    for x in requestDict:
      if x in rec and str(requestDict[x]) != '':
        rec[x] = requestDict[x] # set only known not empty fields
    if not rec['MasterID']:
      rec['RequestPDG'] = '(not confirmed)'
      rec['RequestState']  = 'New'
    else:
      rec['RequestPDG'] = None
      rec['RequestState'] = None

    recl = [ rec[x] for x in self.requestFields[1:-4] ]
    result = self._fixedEscapeValues(recl)
    if not result['OK']:
      return result
    recls = result['Value']
    
    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']

    if rec['MasterID']: # have to check ParentID and MasterID consistency
      try:
        masterID = long(rec['MasterID'])
      except Exception,e:
        self.lock.release()
        return S_ERROR('MasterID is not a number')
      if not rec['ParentID']:
        self.lock.release()
        return S_ERROR('MasterID can not be without ParentID')
      try:
        parentID = long(rec['ParentID'])
      except Exception,e:
        self.lock.release()
        return S_ERROR('ParentID is not a number')
      result = self.__checkMaster(masterID,parentID,connection)
      if not result['OK']:
        return result
      if result['Value'] != 'New':
        self.lock.release()
        return S_ERROR('Requests in progress can not be modified')
    elif rec['ParentID']:
      try:
        parentID = long(rec['ParentID'])
      except Exception,e:
        self.lock.release()
        return S_ERROR('ParentID is not a number')

    req ="INSERT INTO ProductionRequests ( "+','.join(self.requestFields[1:-4])
    req+= " ) VALUES ( %s );" % ','.join(recls)
    result = self._update(req,connection)
    if not result['OK']:
      self.lock.release()
      return result
    req = "SELECT LAST_INSERT_ID();"
    result = self._query(req,connection)
    if not result['OK']:
      self.lock.release()
      return result
    requestID = int(result['Value'][0][0])

    # Update history for masters. Errors are not reported back to the user.
    if rec['RequestState']:
      result = self._update("INSERT INTO RequestHistory ("+
                            ','.join(self.historyFields[:-1])+
                            ") VALUES ( %s,'%s','%s')" %
                            (requestID,str(rec['RequestState']),
                             str(rec['RequestAuthor'])),connection)
      if not result['OK']:
        gLogger.error(result['Message'])
    self.lock.release()
    return S_OK(requestID)

  def __addMonitoring(self,req):
    """ Append monitoring columns. Somehow tricky SQL.
        Most probable need optimizations, but ok for now.
    """
    r ="SELECT t.*,CAST(COALESCE(SUM(sr.NumberOfEvents),0)+"
    r+="                COALESCE(t.NumberOfEvents,0) AS SIGNED)"
    r+="           AS rqTotal FROM "
    r+=" (SELECT t.*,CAST(COALESCE(SUM(pp.BkEvents),0)+"
    r+="                  COALESCE(t.bk,0) AS SIGNED) AS bkTotal FROM "
    r+="  (SELECT t.*, CAST(SUM(pp.BkEvents) AS SIGNED)"
    r+="               AS bk FROM (%s) as t " % req
    r+="   LEFT JOIN ProductionProgress as pp ON pp.RequestID=t.RequestID "
    r+="   WHERE COALESCE(pp.Used,1) GROUP BY t.RequestID) as t "
    r+="  LEFT JOIN ProductionRequests AS sr ON t.RequestID=sr.MasterID "
    r+="  LEFT JOIN ProductionProgress AS pp ON sr.RequestID=pp.RequestID "
    r+="  WHERE COALESCE(pp.Used,1) GROUP BY t.RequestID) AS t "
    r+=" LEFT JOIN ProductionRequests as sr ON sr.MasterID=t.RequestID "
    r+=" GROUP BY t.RequestID"
    return r

  def getProductionRequest(self,requestIDList,subrequestsFor=0,
                           sortBy='',sortOrder='ASC',
                           offset=0,limit=0):
    """ Get the Production Request(s) details.
        If requestIDList is not empty, only productions
        from the list are retured. Otherwise
        master requests are returned (without subrequests) or
        all subrequests of 'subrequestsFor' (when specified).
        Parameters with explicit types are assumed checked by service.
    """
    try: # test parameters 
      for x in requestIDList:
        y = long(x)
    except:
      return S_ERROR("Bad parameters (all request IDs must be numbers)")
    if sortBy:
      if not sortBy in self.requestFields[:-4]:
        return S_ERROR("sortBy field does not exist")
      if sortOrder != 'ASC':
        sortOrder = 'DESC'

    fields = ','.join(['t.'+x for x in self.requestFields[:-4]])
    req = "SELECT %s,COUNT(sr.RequestID) AS HasSubrequest " % fields
    req+= "FROM ProductionRequests as t "
    req+= "LEFT JOIN ProductionRequests AS sr ON t.RequestID=sr.ParentID "
    req+= "WHERE "
    if requestIDList:
      idlist = ','.join([str(x) for x in requestIDList])
      where = "t.RequestID IN (%s)" % idlist
    else:
      if subrequestsFor:
        where ="t.ParentID=%s" % subrequestsFor
      else:
        where ="t.ParentID IS NULL"
    req+=where
    req+=" GROUP BY t.RequestID"
    if sortBy:
      req += " ORDER BY %s %s" % (sortBy,sortOrder)
    if limit:
      req += " LIMIT %s,%s" % (offset,limit)
    result = self._query(self.__addMonitoring(req))
    if not result['OK']:
      return result

    rows = [dict(zip(self.requestFields,row)) for row in result['Value']]
    total = len(rows)
    if limit:
      result = self._query("SELECT COUNT(*) FROM ProductionRequests AS t"+
                           " WHERE %s" % where)
      if not result['OK']:
        return result
      total = result['Value'][0][0]
    return S_OK({'Rows':rows, 'Total':total})

  def __updateState(self,update,old,creds):
    """ Check that state change is possible and
        make appropriate changes when required.
        Eventually it must be full SM check,
        but for now it is done in the portal.
    """
    if not 'RequestState' in update:
      return S_OK('')
    nst = str(update['RequestState'])
    ost = str(old['RequestState'])
    if nst != 'Signed':
      return S_OK('') # no future checks for now
    # if was signed already, must happened from 'New' state
    # if not yet, from 'Tested' state
    if old['RequestPDG'] and old['RequestPDG'] != '(not confirmed)':
      if ost == 'New':
        return S_OK('')
      return S_ERROR('State change is not possible')
    else:     
      if ost == 'Tested':
        update['RequestPDG'] = creds['User'] # !! Check 'Group' here
        return S_OK('')
      return S_ERROR('Request can be signed after test only')

  def updateProductionRequest(self,requestID,requestDict,creds):
    """ Update existing production request
        In states other than New only state and comments
        are changable.

        TODO: RequestPDG change in ??? state
              Protect fields in subrequests
    """
    fdict = dict.fromkeys(self.requestFields[4:-4],None)
    rec = {}
    for x in requestDict:
      if x in fdict and str(requestDict[x]) != '':
        rec[x] = requestDict[x] # set only known not empty fields

    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']

    result = self.__getState(requestID,connection)
    if not result['OK']:
      return result
    requestState = result['Value']

    fields = ','.join(['t.'+x for x in self.requestFields[:-4]])
    req = "SELECT %s " % fields
    req+= "FROM ProductionRequests as t "
    req+= "WHERE t.RequestID=%s" % requestID
    result = self._query(req,connection)
    if not result['OK']:
      self.lock.release()
      return result
    if not result['Value']:
      self.lock.release()
      return S_ERROR('The request is no longer exist')

    old = dict(zip(self.requestFields[:-4],result['Value'][0]))

    update = {}     # Decide what to update (and if that is required)
    for x in rec:
      if str(rec[x]) == str(old[x]):
        continue
      if requestState != 'New':
        if x != 'RequestState' and x != 'Comments':
          continue
      update[x] = rec[x]

    result = self.__updateState(update,old,creds)
    if not result['OK']:
      self.lock.release()
      return result

    if len(update) == 0:
      self.lock.release()
      return S_OK(requestID) # nothing to update

    recl_fields = update.keys() # we have to escape values... tricky way
    recl = [ update[x] for x in recl_fields ]
    result = self._fixedEscapeValues(recl)
    if not result['OK']:
      self.lock.release()
      return result
    updates = ','.join([x+'='+y for x,y in zip(recl_fields,result['Value'])])

    req = "UPDATE ProductionRequests "
    req+= "SET %s " % updates
    req+= "WHERE RequestID=%s" % requestID
    result = self._update(req,connection)
    if not result['OK']:
      self.lock.release()
      return result

    if 'RequestState' in update:
      result = self._update("INSERT INTO RequestHistory ("+
                            ','.join(self.historyFields[:-1])+
                            ") VALUES ( %s,'%s','%s')" %
                            (requestID,str(update['RequestState']),
                             str(creds['User'])),connection)
      if not result['OK']:
        gLogger.error(result['Message'])

    self.lock.release()
    return S_OK(requestID)

  def __getSubrequestsList(self,id,master,connection):
    """ Return list of all subrequests for this request
        NOTE: it does self.lock.release() in case of errors
    """
    result = self._query("SELECT RequestID "+
                         "FROM ProductionRequests "+
                         "WHERE ParentID=%s and MasterID=%s" % (id,master),
                         connection)
    if not result['OK']:
      self.lock.release()
      return result
    sr=[]
    for x in result['Value']:
      sr.append(x[0])
      result = self.__getSubrequestsList(x[0],master,connection)
      if not result['OK']:
        return result
      sr += result['Value']
    return S_OK(sr)

  def deleteProductionRequest(self,requestID):
    """ Delete existing production.
        Subrequests are deleted.
        Substructure is moved up in the tree.
        Available is New and Rejected states only
    """
    try:
      requestID = long(requestID)
    except Exception,e:
      return S_ERROR('RequestID is not a number')
    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']

    result = self.__getState(requestID,connection)
    if not result['OK']:
      return result
    requestState = result['Value']
    if requestState != 'New' and requestState != 'Rejected':
      self.lock.release()
      return S_ERROR('Can not remove request in processing')
    result = self.__getRequestInfo(requestID,connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    parentID = pinfo['ParentID']
    masterID = pinfo['MasterID']

    upperID = parentID
    if not upperID:
      upperID = "NULL"

    # delete subrequests
    req = ''
    if not masterID: # this request is a master
      # delete history
      result = self._update("DELETE FROM RequestHistory "+
                            "WHERE RequestID=%s" % requestID)
      if not result['OK']:
        self.lock.release()
        return result      
      # delete tracking
      result = self._update("DELETE FROM ProductionProgress "+
                            "WHERE RequestID IN "+
                            "(SELECT RequestID FROM ProductionRequests "+
                            "WHERE RequestID=%s OR MasterID=%s" %
                            (requestID,requestID) + ")",connection)
      if not result['OK']:
        self.lock.release()
        return result
      req = "DELETE FROM ProductionRequests "
      req+= "WHERE MasterID=%s" % requestID
    else: # this request is subrequest
      result = self.__getSubrequestsList(requestID,masterID,connection)
      if not result['OK']:
        return result
      rlist = result['Value']
      # delete tracking
      result = self._update("DELETE FROM ProductionProgress "+
                            "WHERE RequestID IN (%s)" %
                            ','.join([str(x) for x in rlist]+[str(requestID)]))
      if not result['OK']:
        self.lock.release()
        return result
      if len(rlist):
        req = "DELETE FROM ProductionRequests "
        req+= "WHERE RequestID in (%s)" % ','.join([str(x) for x in rlist])
    if req:
      result = self._update(req,connection)
      if not result['OK']:
        self.lock.release()
        return result

    # move substructure
    req = "UPDATE ProductionRequests SET ParentID=%s " % upperID
    req+= "WHERE ParentID=%s" % requestID
    result = self._update(req,connection)
    if not result['OK']:
      self.lock.release()
      return result

    # finally delete us
    req = "DELETE FROM ProductionRequests "
    req+= "WHERE RequestID=%s" % requestID
    result = self._update(req,connection)
    
    self.lock.release()

    if not result['OK']:
        return result
    return S_OK(requestID)


  progressFields = [ 'ProductionID','RequestID','Used','BkEvents' ]

  def getProductionProgress(self,requestID):
    """ return the list of associated productions
        requestID must be Long and already checked
    """
    req = "SELECT * FROM ProductionProgress WHERE RequestID=%s" % requestID
    result = self._query(req)
    if not result['OK']:
      return result

    rows = [dict(zip(self.progressFields,row)) for row in result['Value']]
    total = len(rows)
    return S_OK({'Rows':rows, 'Total':total})
    
  def addProductionToRequest(self,pdict):
    """ Associate production to request.
        Existence of request is checked first.
        TODO: check requestState
    """
    try:
      for x in self.progressFields:
        pdict[x] = long(pdict[x])
    except:
      return S_ERROR('Bad parameters')

    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']

    result = self.__getState(pdict['RequestID'],connection)
    if not result['OK']:
      self.lock.release()
      return result
    requestState = result['Value']

    req ="INSERT INTO ProductionProgress ( "
    req+=','.join(self.progressFields)
    req+=" ) VALUES ( "
    req+=','.join([str(pdict[x]) for x in self.progressFields])
    req+=" )"
    result = self._update(req,connection)
    self.lock.release()
    if not result['OK']:
      return result
    return S_OK(pdict['ProductionID'])

  def removeProductionFromRequest(self,productionID):
    """ Deassociate production.
    """
    req ="DELETE FROM ProductionProgress "
    req+="WHERE ProductionID=%s" % str(productionID)
    result = self._update(req)
    if not result['OK']:
      return result
    return S_OK(productionID)

  def useProductionForRequest(self,productionID,used):
    """ Deassociate production.
    """
    used = int(used)
    req ="UPDATE ProductionProgress "
    req+="SET Used=%s " % str(used)
    req+="WHERE ProductionID=%s" % str(productionID)
    result = self._update(req)
    if not result['OK']:
      return result
    return S_OK(productionID)

  def getRequestHistory(self,requestID):
    """ return the list of state changes for the requests
        requestID must be Long and already checked
    """
    req = "SELECT * FROM RequestHistory WHERE RequestID=%s " % requestID
    req+= "ORDER BY TimeStamp"
    result = self._query(req)
    if not result['OK']:
      return result

    rows = [dict(zip(self.historyFields,row)) for row in result['Value']]
    total = len(rows)
    return S_OK({'Rows':rows, 'Total':total})

  def getTrackedProductions(self):
    """ return a list of all productions associated
        with requests in 'Active' state
    """
    req1 = "SELECT RequestID FROM ProductionRequests WHERE RequestState='Active'"
    req2 = "SELECT RequestID FROM ProductionRequests WHERE RequestState='Active' "
    req2+= " OR MasterID in (%s)" % req1
    req  = "SELECT ProductionID FROM ProductionProgress WHERE RequestID "
    req += "in (%s) AND Used" % req2

    result = self._query(req)
    if not result['OK']:
      return result
    values = [row[0] for row in result['Value']]
    return S_OK(values)

  def updateTrackedProductions(self,update):
    """ update tracked productions """
    # check parameters
    try:
      for x in update:
        x['ProductionID'] = long(x['ProductionID'])
        x['BkEvents'] = long(x['BkEvents'])
    except:
      return S_ERROR('Bad parameters')

    for x in update:
      result = self._update("UPDATE ProductionProgress "+
                            "SET BkEvents=%s " % x['BkEvents'] +
                            "WHERE ProductionID=%s" % x['ProductionID'])
      if not result['OK']:
        gLogger.info('Problem in updating progress. Not fatal: %s' %
                     result['Message'])
    return S_OK('')
