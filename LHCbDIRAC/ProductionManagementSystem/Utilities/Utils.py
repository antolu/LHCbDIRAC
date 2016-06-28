""" Just couple utilities
"""

import os
import sqlite3
from DIRAC import gConfig, gLogger
from DIRAC.ConfigurationSystem.Client.Helpers.Registry import getUserOption
from DIRAC.FrameworkSystem.Client.NotificationClient import NotificationClient
from DIRAC.ConfigurationSystem.Client import PathFinder

__RCSID__ = "$Id$"

def informPeople( rec, oldstate, state, author, inform ):
  """ inform utility
  """

  if 'DIRAC' in os.environ:
    cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ProductionManagement/cache.db' )
  else:
    cacheFile = os.path.realpath('cache.db')

  if not state or state == 'New':
    return # was no state change or resurrect

  reqId = rec['RequestID']
  csS = PathFinder.getServiceSection( 'ProductionManagement/ProductionRequest' )
  if not csS:
    gLogger.error( 'No ProductionRequest section in configuration' )
    return

  fromAddress = gConfig.getValue( '%s/fromAddress' % csS, '' )
  if not fromAddress:
    gLogger.error( 'No fromAddress is defined in CS path %s/fromAddress' % csS )
    return
  sendNotifications = gConfig.getValue( '%s/sendNotifications' % csS, 'Yes' )
  if sendNotifications != 'Yes':
    gLogger.info( 'No notifications will be send' )
    return

  footer = "\n\nNOTE: it is an automated notification."
  footer += " Don't reply please.\n"

  footer += "DIRAC Web portal: https://lhcb-portal-dirac.cern.ch/DIRAC/s:%s/g:" % \
           PathFinder.getDIRACSetup()

  ppath = "/?view=tabs&theme=Grey&url_state=1|*LHCbDIRAC.ProductionRequestManager.classes.ProductionRequestManager:,\n\n"

  ppath += 'The request details:\n'
  ppath += '  Type: %s' % str( rec['RequestType'] )
  ppath += '  Name: %s\n' % str( rec['RequestName'] )
  ppath += '  Conditions: %s\n' % str( rec['SimCondition'] )
  ppath += '  Processing pass: %s\n' % str( rec['ProPath'] )

  gLogger.info( ".... %s ...." % ppath )

  authorMail = getUserOption( author, 'Email' )
  if authorMail:
    if not state in ['BK Check', 'Submitted']:
      if state == 'BK OK':
        subj = 'DIRAC: please resign your Production Request %s' % reqId
        body = '\n'.join( ['Customized Simulation Conditions in your request was registered.',
                           'Since Bookkeeping expert could make changes in your request,',
                           'you are asked to confirm it.'] )
      else:
        subj = "DIRAC: the state of Production Request %s is changed to '%s'" % ( reqId, state )
        body = '\n'.join( ['The state of your request is changed.',
                           'This mail is for information only.'] )
      notification = NotificationClient()
      res = notification.sendMail( authorMail, subj,
                                   body + footer + 'lhcb_user' + ppath,
                                   fromAddress, True )
      if not res['OK']:
        gLogger.error( "_inform_people: can't send email: %s" % res['Message'] )

  if inform:
    subj = "DIRAC: the state of %s Production Request %s is changed to '%s'" % ( rec['RequestType'], reqId, state )
    body = '\n'.join( ['You have received this mail because you are'
                       'in the subscription list for this request'] )
    for x in inform.replace( " ", "," ).split( "," ):
      if x:
        if x.find( "@" ) > 0:
          eMail = x
        else:
          eMail = getUserOption( x, 'Email' )
        if eMail:
          notification = NotificationClient()
          res = notification.sendMail( eMail, subj,
                                       body + footer + 'lhcb_user' + ppath,
                                       fromAddress, True )
          if not res['OK']:
            gLogger.error( "_inform_people: can't send email: %s" % res['Message'] )

  if state == 'Accepted':
    subj = "DIRAC: the Production Request %s is accepted." % reqId
    body = '\n'.join( ["The Production Request is signed and ready to process",
                       "You are informed as member of %s group"] )
    groups = [ 'lhcb_prmgr' ]
  elif state == 'BK Check':
    subj = "DIRAC: new %s Production Request %s" % ( rec['RequestType'], reqId )
    body = '\n'.join( ["New Production is requested and it has",
                       "customized Simulation Conditions.",
                       "As member of %s group, your are asked either",
                       "to register new Simulation conditions",
                       "or to reject the request", "",
                       "In case some other member of the group has already",
                       "done that, please ignore this mail."] )
    groups = [ 'lhcb_bk' ]

  elif state == 'Submitted':
    subj = "DIRAC: new %s Production Request %s" % ( rec['RequestType'], reqId )
    body = '\n'.join( ["New Production is requested",
                       "As member of %s group, your are asked either to sign",
                       "or to reject it.", "",
                       "In case some other member of the group has already",
                       "done that, please ignore this mail."] )
    groups = [ 'lhcb_ppg', 'lhcb_tech' ]
  elif state == 'PPG OK' and oldstate == 'Accepted':
    subj = "DIRAC: returned Production Request %s" % reqId
    body = '\n'.join( ["Production Request is returned by Production Manager.",
                       "As member of %s group, your are asked to correct and sign",
                       "or to reject it.", "",
                       "In case some other member of the group has already",
                       "done that, please ignore this mail."] )
    groups = [ 'lhcb_tech' ]
  else:
    return

  with sqlite3.connect(cacheFile) as conn:

    try:
      conn.execute('''CREATE TABLE IF NOT EXISTS ProductionManagementCache(
                    reqId VARCHAR(64) NOT NULL DEFAULT "",
                    thegroup VARCHAR(64) NOT NULL DEFAULT "",
                    subject VARCHAR(64) NOT NULL DEFAULT "",
                    body VARCHAR(254) NOT NULL DEFAULT "",
                    fromAddress VARCHAR(64) NOT NULL DEFAULT ""
                   );''')

    except sqlite3.OperationalError:
      gLogger.error('Email cache database is locked')

    for group in groups:
      conn.execute("INSERT INTO ProductionManagementCache (reqId, thegroup, subject, body, fromAddress)"
                   " VALUES (?, ?, ?, ?, ?)", (reqId, group, subj, (body % group + footer + group + ppath), fromAddress)
                  )

      conn.commit()
