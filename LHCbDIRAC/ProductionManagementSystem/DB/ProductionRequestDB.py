# $Id$
"""
    DIRAC ProductionRequestDB class is a front-end to the repository
    database containing Production Requests and other related tables.
"""
__RCSID__ = "$Revision: 1.23 $"

# Defined states:
#'New'
#'BK OK'
#'Rejected'
#'BK Check'
#'Submitted'
#'PPG OK'
#'On-hold'
#'Tech OK'
#'Accepted'
#'Active'
#'Done'
#'Cancelled'

import string
import time
from DIRAC.Core.Base.DB import DB
from DIRAC.ConfigurationSystem.Client.Config import gConfig
from DIRAC  import gLogger, S_OK, S_ERROR, gConfig
from DIRAC.Core.Utilities import Time
from DIRAC.Core.Utilities import List
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.ConfigurationSystem.Client import PathFinder
from DIRAC.Core.DISET.RPCClient import RPCClient

import threading
import types

import cPickle

def _getMailAddress(user):
  res = gConfig.getOptionsDict('/Registry/Users/%s' % user)
  if not res['OK']:
    gLogger.error('_inform_people: User %s not found in the CS.' % user)
    return ''
  userProp = res['Value']
  if not 'Email' in userProp:
    gLogger.error('_inform_people: User %s has no EMail set in CS.' % user)
    return ''
  return userProp['Email']

def _getMemberMails(group):
  res = gConfig.getOptionsDict('/Registry/Groups/%s' % group)
  if not res['OK']:
    gLogger.error('_inform_people: group %s is not found in CS.' % group)
    return []
  groupProp = res['Value']
  members = groupProp.get('Users','')
  if not members:
    gLogger.error('_inform_people: group %s has no members.' % group)
    return []
  members = List.fromChar(members)
  emails = []
  for user in members:
    email = _getMailAddress(user)
    if email:
      emails.append(email)
  return emails

def _inform_people(rec,oldstate,state,author,inform):
  if not state or state == 'New':
    return # was no state change or resurrect

  id = rec['RequestID']
  csS=PathFinder.getServiceSection( 'ProductionManagement/ProductionRequest' )
  if not csS:
    gLogger.error('No ProductionRequest section in configuration')
    return

  fromAddress = gConfig.getValue('%s/fromAddress' % csS,'')
  if not fromAddress:
    gLogger.error('No fromAddress is defined in CS path %s/fromAddress'%csS)
    return
  sendNotifications = gConfig.getValue('%s/sendNotifications' % csS,'Yes')
  if sendNotifications != 'Yes':
    gLogger.info('No notifications will be send')
    return

  footer = "\n\nNOTE: it is an automated notification."
  footer+= " Don't reply please.\n"
  footer+= "DIRAC Web portal: https://lhcbweb.pic.es/DIRAC/%s/" % \
           PathFinder.getDIRACSetup()
  ppath = '/Production/ProductionRequest/display\n\n'

  ppath+= 'The request details:\n'
  ppath+= '  Type: %s' % str(rec['RequestType'])
  ppath+= '  Name: %s\n' % str(rec['RequestName'])
  ppath+= '  Conditions: %s\n' % str(rec['SimCondition'])
  ppath+= '  Processing pass: %s\n' % str(rec['ProPath'])

  gLogger.info(".... %s ...." %ppath)

  authorMail = _getMailAddress(author)
  if authorMail:
    if not state in ['BK Check','Submitted']:
      if state == 'BK OK':
        subj = 'DIRAC: please resign your Production Request %s' % id
        body =  '\n'.join(
          ['Customized Simulation Conditions in your request was registered.',
           'Since Bookkeeping expert could make changes in your request,',
           'you are asked to confirm it.'])
      else:
        subj = "DIRAC: the state of Production Request %s is changed to '%s'"%\
               (id,state)
        body = '\n'.join(
          ['The state of your request is changed.',
           'This mail is for information only.'])
      notification = NotificationClient()
      res = notification.sendMail(authorMail,subj,
                                  body+footer+'lhcb_user'+ppath,
                                  fromAddress,True)
      if not res['OK']:
        gLogger.error("_inform_people: can't send email: %s" % res['Message'])

  if inform:
    subj = "DIRAC: the state of %s Production Request %s is changed to '%s'"%\
           (rec['RequestType'],id,state)
    body = '\n'.join(
      ['You have received this mail because you are'
       'in the subscription list for this request'])
    for x in inform.replace(" ",",").split(","):
      if x:
        if x.find("@") > 0:
          eMail = x
        else:
          eMail = _getMailAddress(author)
      if eMail:
        notification = NotificationClient()
        res = notification.sendMail(eMail,subj,
                                    body+footer+'lhcb_user'+ppath,
                                    fromAddress,True)
        if not res['OK']:
          gLogger.error("_inform_people: can't send email: %s" % res['Message'])

  if state == 'Accepted':
    subj = "DIRAC: the Production Request %s is accepted." % id
    body = '\n'.join(["The Production Request is signed and ready to process",
                      "You are informed as member of %s group"])
    groups = [ 'lhcb_prmgr' ]
  elif state == 'BK Check':
    subj = "DIRAC: new %s Production Request %s" % (rec['RequestType'],id)
    body = '\n'.join(["New Production is requested and it has",
                      "customized Simulation Conditions.",
                      "As member of %s group, your are asked either",
                      "to register new Simulation conditions",
                      "or to reject the request","",
                      "In case some other member of the group has already",
                      "done that, please ignore this mail."])
    groups = [ 'lhcb_bk' ]

  elif state == 'Submitted':
    subj = "DIRAC: new %s Production Request %s" % (rec['RequestType'],id)
    body = '\n'.join(["New Production is requested",
                      "As member of %s group, your are asked either to sign",
                      "or to reject it.","",
                      "In case some other member of the group has already",
                      "done that, please ignore this mail."])
    groups = [ 'lhcb_ppg','lhcb_tech' ]
  elif state == 'PPG OK' and oldstate == 'Accepted':
    subj = "DIRAC: returned Production Request %s" %id
    body = '\n'.join(["Production Request is returned by Production Manager.",
                      "As member of %s group, your are asked to correct and sign",
                      "or to reject it.","",
                      "In case some other member of the group has already",
                      "done that, please ignore this mail."])
    groups = [ 'lhcb_tech' ]
  else:
    return
  for group in groups:
    for man in _getMemberMails(group):
      notification = NotificationClient()
      res = notification.sendMail(man,subj,
                                  body%group+footer+group+ppath,
                                  fromAddress,True)
      if not res['OK']:
        gLogger.error("_inform_people: can't send email: %s"%res['Message'])
  return



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
                    'Inform','RealNumberOfEvents',
                    'HasSubrequest', 'bk', 'bkSrTotal', 'bkTotal', # runtime
                    'rqTotal', 'crTime', 'upTime' ] # runtime

  historyFields = [ 'RequestID', 'RequestState', 'RequestUser', 'TimeStamp' ]

# !!! current _escapeValues is buggy !!! None and not using connection...
# _insert use it, so I can't...
  def _fixedEscapeValues(self,inValues):
    result = self._escapeValues(inValues)
    if not result['OK']:
      return result
    outValues = result['Value']
    for i,x in enumerate(outValues):
      if x == 'None' or str(x) == '':
        outValues[i] = 'NULL'
    return S_OK(outValues)

  def __prefixComments(self,update,old,user):
    """ Add Log style prefix to the record like change """
    if not update:
      return update
    if not old:
      old=''
    if not update.startswith(old) and not update.endswith(old):
      return update
    prefix = "Comment by %s on %s:\n" % (user,time.strftime("%b %d, %Y"))
    if update.startswith(old):
      if old.rstrip():
        old=old.rstrip()+'\n\n'
      return old+prefix+update[len(old):].lstrip()
    return prefix+update.lstrip()

  def __getRequestInfo(self,id,connection):
    """ Retrive info fields from specified ID
        Used to get ParentID information.
        id must be checked before
        NOTE: it does self.lock.release() in case of errors
    """
    inFields  = [ 'RequestState','ParentID','MasterID','RequestAuthor','Inform' ]
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

  def __getStateAndAuthor(self,id,connection):
    """ Return state, Author and inform list of Master for id (or id's own if no parents)
        id must be checked before
        NOTE: it does self.lock.release() in case of errors
    """
    result = self.__getRequestInfo(id,connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    if not pinfo['MasterID']:
      return S_OK([pinfo['RequestState'],pinfo['RequestAuthor'],pinfo['Inform']])
    result = self.__getRequestInfo(pinfo['MasterID'],connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    return S_OK([pinfo['RequestState'],pinfo['RequestAuthor'],pinfo['Inform']])

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

  def createProductionRequest(self, requestDict, creds):
    """ Create new Production Request
        TODO: Complete check of content
    """
    if creds['Group'] == 'hosts':
      return S_ERROR('Authorization required')

    rec = dict.fromkeys(self.requestFields[1:-7],None)
    for x in requestDict:
      if x in rec and str(requestDict[x]) != '':
        rec[x] = requestDict[x] # set only known not empty fields
    if rec['NumberOfEvents']: # Set RealNumberOfEvents if specified
      try:
        num = long(rec['NumberOfEvents'])
        if num > 0:
          rec['RealNumberOfEvents'] = num
      except:
        pass
    if not rec['MasterID']:
      rec['RequestPDG'] = ''
      if not rec['RequestState']:
        rec['RequestState']  = 'New'
    else:
      rec['RequestPDG'] = None
      rec['RequestState'] = None

    if rec['RequestState']:
      if not rec['RequestState'] in ['New','BK Check','Submitted']:
        return S_ERROR("The request can't be created in '%s' state" % rec['requestState'])
      if rec['RequestState'] != 'New':
        # !!! full information check must be here, but currently in the JS...
        # so we only check EventType consistency
        if not rec['EventType']:
          return S_ERROR("Please specify Event type/number or add subrequest(s)")
    if 'Comments' in rec:
      rec['Comments'] = self.__prefixComments(rec['Comments'],'',creds['User'])

    recl = [ rec[x] for x in self.requestFields[1:-7] ]
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
      result = self.__getStateAndAuthor(masterID,connection)
      if not result['OK']:
        return result
      requestState,requestAuthor,requestInform = result['Value']
      if requestState != 'New':
        self.lock.release()
        return S_ERROR("Requests can't be modified after submission")
      if requestAuthor != creds['User']:
        self.lock.release()
        return S_ERROR("Only request author can add subrequests")
    elif rec['ParentID']:
      try:
        parentID = long(rec['ParentID'])
      except Exception,e:
        self.lock.release()
        return S_ERROR('ParentID is not a number')
      result = self.__getStateAndAuthor(parentID,connection)
      if not result['OK']:
        return result

    req ="INSERT INTO ProductionRequests ( "+','.join(self.requestFields[1:-7])
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
    if rec['RequestState'] in ['BK Check','Submitted']:
      rec['RequestID'] = requestID
      _inform_people(rec,'',rec['RequestState'],creds['User'],rec['Inform'])
    return S_OK(requestID)

  def __addMonitoring(self,req,order):
    """ Append monitoring columns. Somehow tricky SQL.
        Most probable need optimizations, but ok for now.
    """
    r ="SELECT t.*,MIN(rh.TimeStamp) AS crTime,"
    r+="           MAX(rh.TimeStamp) AS upTime "
    r+="FROM "
    r+="(SELECT t.*,CAST(COALESCE(SUM(sr.RealNumberOfEvents),0)+"
    r+="                COALESCE(t.RealNumberOfEvents,0) AS SIGNED)"
    r+="           AS rqTotal "
    r+=" FROM "
    r+=" (SELECT t.*,CAST(COALESCE(SUM(t.bkSrTotal),0)+"
    r+="                  COALESCE(t.bk,0) AS SIGNED) AS bkTotal FROM "
    r+="  (SELECT t.*,CAST(LEAST(COALESCE(SUM(pp.BkEvents),0),"
    r+="                   COALESCE(sr.RealNumberOfEvents,0)) AS SIGNED)"
    r+="              AS bkSrTotal FROM "
    r+="   (SELECT t.*, CAST(SUM(pp.BkEvents) AS SIGNED)"
    r+="                AS bk FROM (%s) as t " % req
    r+="    LEFT JOIN ProductionProgress as pp ON (pp.RequestID=t.RequestID "
    r+="    AND pp.Used=1) GROUP BY t.RequestID) as t "
    r+="   LEFT JOIN ProductionRequests AS sr ON t.RequestID=sr.MasterID "
    r+="   LEFT JOIN ProductionProgress AS pp ON (sr.RequestID=pp.RequestID "
    r+="   AND pp.Used=1) GROUP BY t.RequestID,sr.RequestID) AS t"
    r+="  GROUP BY t.RequestID) AS t"
    r+=" LEFT JOIN ProductionRequests as sr ON sr.MasterID=t.RequestID "
    r+=" GROUP BY t.RequestID) as t"
    r+=" LEFT JOIN RequestHistory as rh ON rh.RequestID=t.RequestID "
    r+=" GROUP BY t.RequestID"
    return r+order

  def getProductionRequest(self,requestIDList,subrequestsFor=0,
                           sortBy='',sortOrder='ASC',
                           offset=0,limit=0,filter={}):
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
    try: # test filters
      sfilter = []
      for x in filter:
        if not x in self.requestFields[:-7]:
          return S_ERROR("bad field in filter")
        val = str(filter[x])
        if val:
          val="( "+','.join(['"'+y+'"' for y in val.split(',')])+") "
          if x == "RequestID":
            # Collect also masters masters
            req = "SELECT RequestID,MasterID FROM ProductionRequests "
            req+= "WHERE RequestID IN %s" % val
            result = self._query(req)
            if not result['OK']:
              return result
            val = []
            for y in result['Value']:
              if str(y[1]) != 'None':
                val.append(str(y[1]))
              else:
                val.append(str(y[0]))
            if not val:
              return S_OK({'Rows':[], 'Total':0})
            val="( "+','.join(val)+") "
          sfilter.append(" t.%s IN %s " %(x,val))
      sfilter=" AND ".join(sfilter)
    except Exception,e:
      return S_ERROR("Bad filter content "+str(e))

    if sortBy:
      if not sortBy in self.requestFields[:-7]:
        return S_ERROR("sortBy field does not exist")
      if sortOrder != 'ASC':
        sortOrder = 'DESC'

    fields = ','.join(['t.'+x for x in self.requestFields[:-7]])
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
      elif sfilter:
        where = sfilter
      else:
        where ="t.ParentID IS NULL"
    req+=where
    req+=" GROUP BY t.RequestID"
    order=""
    if sortBy:
      # order have to be applyed twice: before LIMIT and at the end
      order= " ORDER BY %s %s" % (sortBy,sortOrder)
      req += order
    if limit:
      req += " LIMIT %s,%s" % (offset,limit)
    result = self._query(self.__addMonitoring(req,order))
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

  def __checkUpdate(self,update,old,creds,connection):
    """ Check that update is possible.
        Return dict with values for _inform_people (with
        state=='' in  case notification is not required)
        NOTE: unlock in case of errors
    """
    requestID = old['RequestID']
    result = self.__getStateAndAuthor(requestID,connection)
    if not result['OK']:
      return result
    requestState,requestAuthor,requestInform = result['Value']
    rec=old.copy()
    rec.update(update)
    inform = { 'rec':rec, 'state':'', 'author': str(requestAuthor),
               'oldstate': requestState, 'inform': requestInform }

    hasSubreq = False
    if not old['MasterID']:
      result = self._query("SELECT RequestID "+
                           "FROM ProductionRequests "+
                           "WHERE MasterID=%s" % requestID,connection)
      if not result['OK']:
        self.lock.release()
        return result
      if result['Value']:
        hasSubreq = True

    if creds['Group'] == 'diracAdmin':
      return S_OK(inform)

    if requestState in ['Done','Cancelled'] and creds['Group'] != 'diracAdmin':
      self.lock.release()
      return S_ERROR("Done or cancelled requests can't be modified")

    # Check that a person can update in general (that also means he can
    # change at least comments)
    if requestState in ['New','BK OK','Rejected']:
      if requestAuthor != creds['User']:
        self.lock.release()
        return S_ERROR("Only author is allowed to modify unsubmitted request")
    elif requestState == 'BK Check':
      if creds['Group'] != 'lhcb_bk':
        self.lock.release()
        return S_ERROR("Only BK expert can manage new Simulation Conditions")
    elif requestState == 'Submitted':
      if creds['Group'] != 'lhcb_ppg' and creds['Group'] != 'lhcb_tech':
        self.lock.release()
        return S_ERROR("Only PPG members or Tech. experts are allowed to sign submitted request")
    elif requestState == 'PPG OK':
      if creds['Group'] != 'lhcb_tech':
        self.lock.release()
        return S_ERROR("Only Tech. experts are allowed to sign this request")
    elif requestState == 'On-hold':
      if creds['Group'] != 'lhcb_tech':
        self.lock.release()
        return S_ERROR("Only Tech. experts are allowed to sign this request")
    elif requestState == 'Tech OK':
      if creds['Group'] != 'lhcb_ppg':
        self.lock.release()
        return S_ERROR("Only PPG members are allowed to sign this request")
    elif requestState == 'Accepted':
      if creds['Group'] != 'lhcb_prmgr':
        self.lock.release()
        return S_ERROR("Only Tech. experts are allowed to manage accepted request")
    elif requestState == 'Active':
      if not creds['Group'] in ['lhcb_prmgr','lhcb_prod']:
        self.lock.release()
        return S_ERROR("Only experts are allowed to manage active request")
    elif requestState in ['Done','Cancelled']:
      if creds['Group'] != 'diracAdmin':
        self.lock.release()
        return S_ERROR("Only admin can violate the system logic")
    else:
      self.lock.release()
      return S_ERROR("The request is in unknown state '%s'" % requestState)

    if old['MasterID']: # for subrequests it's simple
      if requestState == 'New':
        for x in update:
          if not x in ['EventType','NumberOfEvents','RealNumberOfEvents','Comments']:
            self.lock.release()
            return S_ERROR("%s is not allowed in subrequests" % x)
          if x != 'Comments' and not update[x]:
            self.lock.release()
            return S_ERROR("You must specify event type and number")
      else:
        if len(update)!=1 or not 'Comments' in update:
          self.lock.release()
          return S_ERROR("Only comments can be changed for subrequest in progress")
      return S_OK(inform)
    # for masters it is more complicated...
    if requestState == 'New':
      if not 'RequestState' in update:
        return S_OK(inform)
      if update['RequestState'] in ['BK Check','Submitted']:
        # !!! full information check must be here, but currently in the JS...
        # so we only check EventType consistency
        eventType = old['EventType']
        if 'EventType' in update:
          eventType = update['EventType']
        # gLogger.error(str(update))
        if eventType and hasSubreq:
          self.lock.release()
          return S_ERROR("The request has subrequests, so it must not specify Event type")
        if not eventType and not hasSubreq:
          self.lock.release()
          return S_ERROR("Please specify Event type/number or add subrequest(s)")
      else:
        self.lock.release()
        return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
    elif requestState == 'Rejected':
      if len(update)!=1 or update.get('RequestState','') != 'New':
        self.lock.release()
        return S_ERROR("Rejected requests must be resurrected before modifications")
    elif requestState == 'BK Check':
      for x in update:
        if not x in ['RequestState','SimCondition','SimCondID','SimCondDetail','Comments','Inform']:
          self.lock.release()
          return S_ERROR("%s can't be modified during BK check" % x)
      if not 'RequestState' in update:
        return S_OK(inform)
      if not update['RequestState'] in ['BK OK','Rejected']:
        self.lock.release()
        return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
      if update['RequestState'] == 'BK OK' and not update.get('SimCondID',old['SimCondID']):
        self.lock.release()
        return S_ERROR("Registered simulation conditions required to sign for BK OK")
    elif requestState == 'BK OK':
      for x in update:
        if not x in ['RequestState','Comments','Inform']:
          self.lock.release()
          return S_ERROR("%s can't be modified after BK check" % x)
      if not 'RequestState' in update:
        return S_OK(inform)
      if not update['RequestState'] in ['Submitted','Rejected']:
        self.lock.release()
        return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
    elif requestState == 'Submitted':
      if creds['Group'] == 'lhcb_ppg':
        for x in update:
          if not x in ['RequestState','Comments','Inform','RequestPriority']:
            self.lock.release()
            return S_ERROR("%s can't be modified during PPG signing" % x)
        if not 'RequestState' in update:
          return S_OK(inform)
        if update['RequestState'] == 'Accepted':
          update['RequestState'] = 'PPG OK';
        if not update['RequestState'] in ['PPG OK','Rejected']:
          self.lock.release()
          return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
      if creds['Group'] == 'lhcb_tech':
        for x in update:
          if not x in ['RequestState','Comments','Inform','ProPath','ProID','ProDetail']:
            self.lock.release()
            return S_ERROR("%s can't be modified during Tech signing" % x)
        if not 'RequestState' in update:
          return S_OK(inform)
        if update['RequestState'] == 'Accepted':
          update['RequestState'] = 'Tech OK';
        if not update['RequestState'] in ['Tech OK','Rejected']:
          self.lock.release()
          return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
# AZ: removed from logic
#        if update['RequestState'] == 'Tech OK' and not update.get('ProID',old['ProID']):
#          self.lock.release()
#          return S_ERROR("Registered processing pass is required to sign for Tech OK")
    elif requestState in ['PPG OK','On-hold']:
      for x in update:
        if not x in ['RequestState','Comments','Inform','ProPath','ProID','ProDetail']:
          self.lock.release()
          return S_ERROR("%s can't be modified during Tech signing" % x)
      if not 'RequestState' in update:
        return S_OK(inform)
      if update['RequestState'] == 'Tech OK':
        update['RequestState'] = 'Accepted'
      if not update['RequestState'] in ['Accepted','Rejected','On-hold']:
        self.lock.release()
        return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
# AZ: removed from logic
#      if update['RequestState'] == 'Accepted' and not update.get('ProID',old['ProID']):
#        self.lock.release()
#        return S_ERROR("Registered processing pass is required to sign for Tech OK")
    elif requestState == 'Tech OK':
      for x in update:
        if not x in ['RequestState','Comments','Inform','RequestPriority']:
          self.lock.release()
          return S_ERROR("%s can't be modified during PPG signing" % x)
      if not 'RequestState' in update:
        return S_OK(inform)
      if update['RequestState'] == 'PPG OK':
        update['RequestState'] = 'Accepted'
      if not update['RequestState'] in ['Accepted','Rejected']:
        self.lock.release()
        return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
    elif requestState in ['Accepted','Active']:
      for x in update:
        if not x in ['RequestState','Comments','Inform','ProPath','ProID','ProDetail']:
          self.lock.release()
          return S_ERROR("%s can't be modified during the progress" % x)
      if not 'RequestState' in update:
        return S_OK(inform)
      if requestState == 'Accepted':
        if not update['RequestState'] in ['Active','Cancelled','PPG OK']:
          self.lock.release()
          return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
      else:
        if not update['RequestState'] in ['Done','Cancelled']:
          self.lock.release()
          return S_ERROR("The request is '%s' now, moving to '%s' is not possible" % (requestState,update['RequestState']))
    inform['state'] = update['RequestState']
    inform['rec'].update(update)
    return S_OK(inform)

  def updateProductionRequest(self,requestID,requestDict,creds):
    """ Update existing production request
        In states other than New only state and comments
        are changable.

        TODO: RequestPDG change in ??? state
              Protect fields in subrequests
    """
    fdict = dict.fromkeys(self.requestFields[4:-7],None)
    rec = {}
    for x in requestDict:
      if x in fdict:
        if requestDict[x]:
          rec[x] = requestDict[x] # set only known fields
        else:
          rec[x] = None # to be more deterministic...

    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']


    fields = ','.join(['t.'+x for x in self.requestFields[:-7]])
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

    old = dict(zip(self.requestFields[:-7],result['Value'][0]))

    update = {}     # Decide what to update (and if that is required)
    for x in rec:
      if str(rec[x]) == str(old[x]):
        continue
      update[x] = rec[x]

    if len(update) == 0:
      self.lock.release()
      return S_OK(requestID) # nothing to update

    if 'NumberOfEvents' in update: # Update RealNumberOfEvents if specified
      num = 0
      try:
        num = long(rec['NumberOfEvents'])
        if num < 0:
          num = 0
      except:
        pass
      update['RealNumberOfEvents'] = str(num)

    if 'Comments' in update:
      update['Comments'] = self.__prefixComments(update['Comments'],
                                                 old['Comments'],creds['User'])

    result = self.__checkUpdate(update,old,creds,connection)
    if not result['OK']:
      return result
    inform = result['Value']

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
    _inform_people(**inform)
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

  def deleteProductionRequest(self,requestID,creds):
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

    result = self.__getStateAndAuthor(requestID,connection)
    if not result['OK']:
      return result
    requestState,requestAuthor,requestInform = result['Value']
    if creds['Group'] != 'diracAdmin':
      if requestAuthor != creds['User']:
        self.lock.release()
        return S_ERROR('Only author can remove a request')
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

  def __getRequest(self,requestID,connection):
    """ retrive complete request record.
        NOTE: unlock in case of errors
    """
    fields = ','.join(['t.'+x for x in self.requestFields[:-7]])
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
    rec = dict(zip(self.requestFields[:-7],result['Value'][0]))
    return S_OK(rec)

  def __clearProcessingPass(self,rec):
    """ clear processing pass section.
        Processing pass name, first app. with version are untouched
    """
    rec['ProID'] = None
    detail = cPickle.loads(rec['ProDetail'])
    nd = {}
    for x in ['pDsc','p1Ver','p1App']:
      if x in detail:
        nd[x] = detail[x]
    if 'p1App' in nd and 'p1Ver' in nd:
      nd['pAll'] = u'%s-%s' % (nd['p1App'],nd['p1Ver'])
      nd['p1Html'] = u'<b>Pass 1:</b> %s<br>' % nd['pAll']
    rec['ProDetail'] = cPickle.dumps(nd)
    
  def __duplicateDeep(self,requestID,masterID,parentID,creds,connection,
                      clearpp):
    """ recurcive duplication function.
        NOTE: unlock in case of errors
    """

    result = self.__getRequest(requestID,connection)
    if not result['OK']:
      self.lock.release()
      return result
    rec = result['Value']
    if clearpp:
      self.__clearProcessingPass(rec)
  
    if masterID and not rec['MasterID']:
      return S_OK("") # substructured request

    if rec['ParentID']:
      rec['ParentID'] = parentID
    if rec['MasterID']:
      rec['MasterID'] = masterID
    if rec['RequestAuthor']:
      rec['RequestAuthor'] = creds['User']
    if rec['RequestState']:
      rec['RequestState'] = 'New'

    # Clear RealNumberOfEvents if required
    try:
      num = 0
      num = long(rec['NumberOfEvents'])
      if num < 0:
        num = 0
    except:
      pass
    rec['RealNumberOfEvents'] = str(num)

    recl = [ rec[x] for x in self.requestFields[1:-7] ]
    result = self._fixedEscapeValues(recl)
    if not result['OK']:
      self.lock.release()
      return result
    recls = result['Value']

    req ="INSERT INTO ProductionRequests ( "+','.join(self.requestFields[1:-7])
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
    newRequestID = int(result['Value'][0][0])

    # Update history for masters. Errors are not reported back to the user.
    if rec['RequestState']:
      result = self._update("INSERT INTO RequestHistory ("+
                            ','.join(self.historyFields[:-1])+
                            ") VALUES ( %s,'%s','%s')" %
                            (newRequestID,str(rec['RequestState']),
                             str(rec['RequestAuthor'])),connection)
      if not result['OK']:
        gLogger.error(result['Message'])

    # now for subrequests
    if not masterID:
      masterID = newRequestID
    parentID = newRequestID

    req = "SELECT RequestID "
    req+= "FROM ProductionRequests as t "
    req+= "WHERE t.ParentID=%s" % requestID
    result = self._query(req,connection)
    if not result['OK']:
      self.lock.release()
      return result
    for chID in [row[0] for row in result['Value']]:
      result = self.__duplicateDeep(chID,masterID,parentID,creds,
                                    connection,False)
      if not result['OK']:
        return result

    return S_OK(long(newRequestID))


  def duplicateProductionRequest(self,requestID,creds,clearpp):
    """
    Duplicate production request with all it's subrequests
    (but without substructure). If that is subrequest,
    master must be in New state and user must be the
    author. If clearpp is set, all details in the Processing
    pass (of the master) are cleaned.
    """
    if creds['Group'] == 'hosts':
      return S_ERROR('Authorization required')

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

    result = self.__getRequestInfo(requestID,connection)
    if not result['OK']:
      return result
    pinfo = result['Value']
    parentID = pinfo['ParentID']
    masterID = pinfo['MasterID']

    if masterID:
      clearpp = False
      result = self.__getStateAndAuthor(requestID,connection)
      if not result['OK']:
        return result
      requestState,requestAuthor,requestInform = result['Value']
      if requestState != 'New' or requestAuthor != creds['User']:
        self.lock.release()
        return S_ERROR('Can not duplicate subrequest of request in progress')

    result = self.__duplicateDeep(requestID,masterID,parentID,creds,
                                  connection,clearpp)
    if result['OK']:
      self.lock.release()
    return result

  def __checkAuthorizeSplit(self,requestState,creds):
    """
    Check that current user is allowed to split in specified state
    """
    if creds['Group'] == 'diracAdmin':
      return S_OK()
    if ( requestState in [ 'Submitted', 'PPG OK', 'On-hold' ] ) \
           and creds['Group'] == 'lhcb_tech':
      return S_OK()
    if  requestState in [ 'Accepted', 'Active' ] and creds['Group'] == 'lhcb_prmgr':
      return S_OK()
    return S_ERROR('You are not allowed to split the request')

  def __moveChildDeep(self,requestID,masterID,setParent,connection):
    """
    Update parent for this request if setParent is True
    and update master for this and all subrequests
    """
    if setParent:
      updates="ParentID=%s,MasterID=%s" % (str(masterID),str(masterID))
    req = "UPDATE ProductionRequests "
    req+= "SET %s " % updates
    req+= "WHERE RequestID=%s" % requestID
    result = self._update(req,connection)
    if not result['OK']:
      return result
    req = "SELECT RequestID,MasterID "
    req+= "FROM ProductionRequests as t "
    req+= "WHERE t.ParentID=%s" % requestID
    result = self._query(req,connection)
    if not result['OK']:
      return result
    for ch in result['Value']:
      if ch[1]:
        ret = self.__moveChildDeep(ch[0],masterID,False,connection)
        if not ret['OK']:
          gLogger.error("_moveChildDeep: can not move to %s: %s" % (str(masterID),ret['Message']))
        # !!! Failing that will leave both requests inconsistant !!!
        # But since we have only one subrequest level now, it will never happened
    return S_OK()

  def splitProductionRequest(self,requestID,splitlist,creds):
    """
    Fully duplicate master production request with its history
    and reassociate first level subrequests from splitlist
    (with there subrequest structure).
    Substructures can not be moved. 
    Only experts in appropriate request state can request the split.
    """
    try:
      requestID = long(requestID)
    except Exception,e:
      return S_ERROR('RequestID is not a number')
    if not splitlist:
      return S_ERROR('Split list is empty')
    isplitlist = []
    try:
      isplitlist = [ long(x) for x in splitlist ]
    except Exception,e:
      return S_ERROR('RequestID in split list is not a number')
    
    self.lock.acquire() # transaction begin ?? may be after connection ??
    result = self._getConnection()
    if not result['OK']:
      self.lock.release()
      return S_ERROR('Failed to get connection to MySQL: '+result['Message'])
    connection = result['Value']

    result = self.__getRequest(requestID,connection)
    if not result['OK']:
      self.lock.release()
      return result
    rec = result['Value']

    # Check that operation is valid
    if rec['MasterID']:
      self.lock.release()
      return S_ERROR('Can not duplicate subrequest of request in progress')
    result = self.__checkAuthorizeSplit(rec['RequestState'],creds)
    if not result['OK']:
      self.lock.release()
      return result
    req = "SELECT RequestID,MasterID "
    req+= "FROM ProductionRequests as t "
    req+= "WHERE t.ParentID=%s" % requestID
    result = self._query(req,connection)
    if not result['OK']:
      self.lock.release()
      return result
    fisplitlist = []
    keeplist = []
    for ch in result['Value']:
      if ch[0] in isplitlist and ch[1] == requestID:
        fisplitlist.append(long(ch[0]))
      elif ch[1]:
        keeplist.append(long(ch[0]))
    if len(isplitlist) != len(fisplitlist):
      self.lock.release()
      return S_ERROR('Requested for spliting subrequests are no longer exist')
    if not keeplist:
      self.lock.release()
      return S_ERROR('You have to keep at least one subrequest')

    # Now copy the master
    recl = [ rec[x] for x in self.requestFields[1:-7] ]
    result = self._fixedEscapeValues(recl)
    if not result['OK']:
      self.lock.release()
      return result
    recls = result['Value']
    req ="INSERT INTO ProductionRequests ( "+','.join(self.requestFields[1:-7])
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
    newRequestID = long(result['Value'][0][0])

    # Move subrequests (!! Errors are not fatal !!)
    rsplitlist = []
    for x in fisplitlist:
      if self.__moveChildDeep(x,newRequestID,True,connection)['OK']:
        rsplitlist.append(x)
    # If nothing could be moved, remove the master
    if not rsplitlist:
      req ="DELETE FROM ProductionRequests "
      req+="WHERE RequestID=%s" % str(newRequestID)
      result = self._update(req,connection)
      self.lock.release()
      return S_ERROR('Could not move subrequests')

    # Copy the history (failures are not fatal since
    # it is hard to revert the previous changes...)
    req = "SELECT * FROM RequestHistory WHERE RequestID=%s " % requestID
    req+= "ORDER BY TimeStamp"
    result = self._query(req,connection)
    if not result['OK']:
      gLogger.error("SplitProductionRequest: can not get history for %s: %s" % (str(requestID),result['Message']))
    else:
      for x in result['Value']:
        x=list(x)
        x[0]= newRequestID
        ret = self._update("INSERT INTO RequestHistory ("+
                           ','.join(self.historyFields)+
                           ") VALUES ( %s,'%s','%s','%s')" % tuple([str(y) for y in x]),connection)
      if not ret['OK']:
        gLogger.error("SplitProductionRequest: add history fail: %s",['Message'])

    self.lock.release()
    return S_OK(newRequestID)

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

    result = self.__getStateAndAuthor(pdict['RequestID'],connection)
    if not result['OK']:
      self.lock.release()
      return result
    requestState,requestAuthor,requestInform = result['Value']

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
    req2 = "SELECT RequestID FROM ProductionRequests WHERE RequestState='Active'"
    req2+= " OR MasterID in (%s)" % req1
    req  = "SELECT ProductionID FROM ProductionProgress WHERE RequestID "
    req += "in (%s)" % req2

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

  def __trackedInputSQL(self,fields):
    req = "SELECT %s " % fields
    req+= "FROM ProductionRequests as t WHERE"
    req+= ' t.RequestState="Active"'
    req+= ' AND NumberOfEvents<0 '
    return self._query(req)

  def getTrackedInput(self):
    """ return a list of all requests with dynamic input
        in 'Active' state
    """

    fields = ','.join(['t.'+x for x in self.requestFields[:-7]])
    result = self.__trackedInputSQL(fields)
    if not result['OK']:
      return result
    rec = []
    for x in result['Value']:
      r = dict(zip(self.requestFields[:-7],x))
      if r['SimCondDetail']:
        r.update(cPickle.loads(r['SimCondDetail']))
      else:
        continue
      del r['SimCondDetail']
      try:
        num = long(r['RealNumberOfEvents'])
      except:
        num = 0
      if num > 0:
        continue
      rec.append(r)
    return S_OK(rec)

  def updateTrackedInput(self,update):
    """ update real number of input events """
    # check parameters
    try:
      for x in update:
        x['RequestID'] = long(x['RequestID'])
        x['RealNumberOfEvents'] = long(x['RealNumberOfEvents'])
    except:
      return S_ERROR('Bad parameters')
    result = self.__trackedInputSQL('RequestID')
    if not result['OK']:
      return result
    allowed = dict.fromkeys([x[0] for x in result['Value']],None)
    skiped = []
    for x in update:
      if not x['RequestID'] in allowed:
        skiped.append(x['RequestID'])
        continue
      req ="UPDATE ProductionRequests "
      req+="SET RealNumberOfEvents=%s " % str(x['RealNumberOfEvents'])
      req+="WHERE RequestID=%s" % str(x['RequestID'])
      result = self._update(req)
      if not result['OK']:
        skiped.append(x['RequestID'])
    if skiped:
      return S_ERROR("updateTrackedInput has skiped requests %s" % \
                     ','.join(skiped))
    return S_OK('')

  def getProductionList(self,requestID):
    """ return a list of all productions associated
        with the request or any its subrequest
    """
    req1 = "SELECT RequestID FROM ProductionRequests WHERE MasterID=%s"%requestID 
    req  = "SELECT ProductionID FROM ProductionProgress WHERE RequestID "
    req += "in (%s) OR RequestID=%s" % (req1,requestID)

    result = self._query(req)
    if not result['OK']:
      return result

    gLogger.info(result['Value'])

    values = [row[0] for row in result['Value']]
    return S_OK(values)

  def getAllSubRequestSummary(self,status='',type=''):
    """ return a dictionary containing a summary for each subrequest
    """
    req = "SELECT RequestID,ParentID,RequestType,RequestState,NumberOfEvents FROM ProductionRequests"
    if status and type:
      req = "%s WHERE RequestState = '%s' AND RequestType = '%s'" % (req,status,type)
    elif status:
      req = "%s WHERE RequestState = '%s'" % (req,status)
    elif type:
      req = "%s WHERE RequestType = '%s'" % (req,type)
    res = self._query(req)
    if not res['OK']:
      return res
    sRequestInfo = {}
    for sRequestID,parentID,type,status,reqEvents in res['Value']:
      if not parentID:
        parent = 0
      sRequestInfo[sRequestID] = {'Master':parent, 'RequestType':type, 'Status':status, 'ReqEvents':reqEvents}
    return S_OK(sRequestInfo)
    
  def getAllProductionProgress(self):
    """ return a dictionary containing for each requestID the active productions and the number of events
    """
    req = "SELECT RequestID, ProductionID, Used, BkEvents FROM ProductionProgress;"
    res = self._query(req)
    if not res['OK']:
      return res
    sRequestInfo = {}
    for sRequestID, prodID, used, events in res['Value']:
      if not sRequestInfo.has_key(sRequestID):
        sRequestInfo[sRequestID] = {}
      sRequestInfo[sRequestID][prodID] = {'Used':used, 'Events':events}
    return S_OK(sRequestInfo)

  def getFilterOptions(self):
    """ Return the dictionary with possible values for filter
    """
    opts = {}
    for x,y in [ ('State','RequestState'),
                 ('Type','RequestType'),
                 ('Author','RequestAuthor'),
                 ('EType','EventType') ] :
      req = "SELECT DISTINCT %s FROM ProductionRequests " % y
      req+= "WHERE %s IS NOT NULL " % y
      req+= "ORDER BY %s" % y
      res = self._query(req);
      if not res['OK']:
        return res
      opts[x] = [row[0] for row in res['Value']]
    return S_OK(opts);
