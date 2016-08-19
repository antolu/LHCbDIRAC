''' NotifyAgent
  This agent reads a cache file ( cache.db ) which contains the aggregated information
  of what happened to each production request. After reading the cache file
  ( by default every 30 minutes ) it sends an email for every site and then clears it.

  Please note that this agent is a hybrid agent that sends aggregated emails for both
  LHCbDIRAC.ProductionManagementSystem.Utilities.Utils.informPeople and
  LHCbDIRAC.ProductionManagementSystem.Agent.ProductionStatusAgent._mailProdManager
'''

import os
import sqlite3
from DIRAC                                                       import gConfig, S_OK
from DIRAC.Core.Base.AgentModule                                 import AgentModule
from DIRAC.FrameworkSystem.Client.NotificationClient             import NotificationClient
from DIRAC.ConfigurationSystem.Client                            import PathFinder
from LHCbDIRAC.ProductionManagementSystem.Utilities.Utils        import _getMemberMails

__RCSID__ = '$Id: $'

AGENT_NAME = 'ProductionManagement/NotifyAgent'


class NotifyAgent( AgentModule ):

  def __init__( self, *args, **kwargs ):

    super(NotifyAgent, self).__init__(args, kwargs)

    self.notification = None
    self.csS = None
    self.fromAddress = None

    if 'DIRAC' in os.environ:
      self.cacheFile = os.path.join( os.getenv('DIRAC'), 'work/ProductionManagement/cache.db' )
    else:
      self.cacheFile = os.path.realpath('cache.db')

  def initialize( self ):
    ''' NotifyAgent initialization
    '''

    self.notification = NotificationClient()

    self.csS = PathFinder.getServiceSection( 'ProductionManagement/ProductionRequest' )

    self.fromAddress = gConfig.getValue( '%s/fromAddress' % self.csS, '' )
    if not self.fromAddress:
      self.log.info( 'No fromAddress is defined, a default value will be used instead' )
      self.fromAddress = 'vladimir.romanovsky@cern.ch'

    return S_OK()

  def execute( self ):

    if not os.path.isfile(self.cacheFile):
      self.log.error( self.cacheFile + " does not exist." )
      return S_OK()

    with sqlite3.connect(self.cacheFile) as conn:

      # *******************************************************
      # This is for the ProductionManagementSystem's Utilities
      # *******************************************************

      link = "https://lhcb-portal-dirac.cern.ch/DIRAC/s:%s/g:" % PathFinder.getDIRACSetup() + \
             "/?view=tabs&theme=Grey&url_state=1|*LHCbDIRAC.ProductionRequestManager.classes.ProductionRequestManager:"

      if not self.csS:
        self.log.error( 'No ProductionRequest section in configuration' )
        return S_OK()

      result = conn.execute("SELECT DISTINCT thegroup from ProductionManagementCache;")

      html_header = """\
            <!DOCTYPE html>
            <html>
            <head>
            <meta charset='UTF-8'>
              <style>
                table{color:#333;font-family:Helvetica,Arial,sans-serif;min-width:850px;border-collapse:collapse;border-spacing:0}
                td,th{border:1px solid transparent;height:30px;transition:all .3s}th{background:#DFDFDF;font-weight:700}
                td{background:#FAFAFA;text-align:center}tr:nth-child(even) td{background:#F1F1F1}tr:nth-child(odd)
                td{background:#FEFEFE}tr td:hover{background:#666;color:#FFF}tr td.link:hover{background:inherit;}
                p{width: 850px;}
              </style>
            </head>
            <body>
            """

      for group in result:

        aggregated_body = ""
        html_elements = ""

        if group[0] == 'lhcb_bk':
          header = "New Productions are requested and they have customized Simulation Conditions. " \
                   "As member of <span style='color:green'>" + group[0] + "</span> group, your are asked either to register new Simulation conditions " \
                   "or to reject the requests. In case some other member of the group has already done that, " \
                   "please ignore this mail.\n"

        elif group[0] in [ 'lhcb_ppg', 'lhcb_tech' ]:
          header = "New Productions are requested. As member of <span style='color:green'>" + group[0] + "</span> group, your are asked either to sign or " \
                   "to reject it. In case some other member of the group has already done that, please ignore this mail.\n"
        else:
          header = "As member of <span style='color:green'>" + group[0] + "</span> group, your are asked to review the below requests.\n"

        cursor = conn.execute("SELECT reqId, reqType, reqName, SimCondition, ProPath from ProductionManagementCache "
                              "WHERE thegroup = ?", (group[0],) )

        for reqId, reqType, reqWG, reqName, SimCondition, ProPath in cursor:

          html_elements += "<tr>" + \
                           "<td>" + reqId + "</td>" + \
                           "<td>" + reqName + "</td>" + \
                           "<td>" + reqType + "</td>" + \
                           "<td>" + reqWG + "</td>" + \
                           "<td>" + SimCondition + "</td>" + \
                           "<td>" + ProPath + "</td>" + \
                           "<td class='link'><a href='" + link + "' target='_blank'> Link </a></td>" + \
                           "</tr>"

        html_body = """\
          <p>{header}</p>
          <table>
            <tr>
                <th>ID</th>
                <th>Name</th>
                <th>Type</th>
                <th>Working Group</th>
                <th>Conditions</th>
                <th>Processing pass</th>
                <th>Link</th>
            </tr>
            {html_elements}
          </table>
        </body>
        </html>
        """.format(header=header, html_elements=html_elements)

        aggregated_body = html_header + html_body

        for people in _getMemberMails( group[0] ):

          res = self.notification.sendMail( people, "Notifications for production requests", aggregated_body, self.fromAddress, True )

          if res['OK']:
            conn.execute("DELETE FROM ProductionManagementCache;")
            conn.execute("VACUUM;")
          else:
            self.log.error( "_inform_people: can't send email: %s" % res['Message'] )
            return S_OK()

      # **************************************
      # This is for the ProductionStatusAgent
      # **************************************

      html_header2 = """\
            <!DOCTYPE html>
            <html>
            <head>
            <meta charset='UTF-8'>
              <style>
                table{color:#333;font-family:Helvetica,Arial,sans-serif;min-width:700px;border-collapse:collapse;border-spacing:0}
                td,th{border:1px solid transparent;height:30px;transition:all .3s}th{background:#DFDFDF;font-weight:700}
                td{background:#FAFAFA;text-align:center}.setup{font-size:150%;color:grey}.Active{color:green}
                .Archived,.Cleaned,.Cleaning{color:gray}.Completed{color:purple}.Idle{color:#90ee90}.Stopped{color:orange}
                .Testing,.TransformationCleaned{color:gray}tr:nth-child(even) td{background:#F1F1F1}
                tr:nth-child(odd) td{background:#FEFEFE}tr td:hover{background:#666;color:#FFF}
              </style>
            </head>
            <body>
            """

      aggregated_body = ""
      html_elements = ""
      html_elements2 = ""

      cursor = conn.execute("SELECT production, from_status, to_status, time from ProductionStatusAgentCache;")

      for production, from_status, to_status, time in cursor:

        html_elements += "<tr>" + \
                         "<td>" + production + "</td>" + \
                         "<td class='" + from_status + "'>" + from_status + "</td>" + \
                         "<td class='" + to_status + "'>" + to_status + "</td>" + \
                         "<td>" + time + "</td>" + \
                         "</tr>"

      html_body1 = """\
        <p class="setup">Transformations updated</p>
        <table>
          <tr>
              <th>Production</th>
              <th>From</th>
              <th>To</th>
              <th>Time</th>
          </tr>
          {html_elements}
        </table>
      </body>
      </html>
      """.format(html_elements=html_elements)

      cursor = conn.execute("SELECT prod_requests, time from ProductionStatusAgentReqCache;")

      for prod_requests, time in cursor:

        html_elements2 += "<tr>" + \
                          "<td>" + prod_requests + "</td>" + \
                          "<td>" + time + "</td>" + \
                          "</tr>"

      html_body2 = """\
        <br />
        <p class="setup">Production Requests updated to Done status</p>
        <table>
          <tr>
              <th>Production Requests</th>
              <th>Time</th>
          </tr>
          {html_elements2}
        </table>
      </body>
      </html>
      """.format(html_elements=html_elements2)

      aggregated_body = html_header2 + html_body1 + html_body2

      res = self.notification.sendMail( 'vladimir.romanovsky@cern.ch', "Transformation Status Updates", aggregated_body,
                                        'vladimir.romanovsky@cern.ch', localAttempt = False )

      if res['OK']:

        conn.execute("DELETE FROM ProductionStatusAgentCache;")
        conn.execute("VACUUM;")

        conn.execute("DELETE FROM ProductionStatusAgentReqCache;")
        conn.execute("VACUUM;")

      else:
        self.log.error( "Can't send email: %s" % res['Message'] )
        return S_OK()

    return S_OK()

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
