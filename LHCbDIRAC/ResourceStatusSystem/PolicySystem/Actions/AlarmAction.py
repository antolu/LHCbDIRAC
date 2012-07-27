# $HeadURL $
'''  AlarmAction

'''

#import urllib2
#
#from DIRAC.ResourceStatusSystem.PolicySystem.Actions.AlarmAction import AlarmAction as BaseAlarmAction
#from DIRAC.ResourceStatusSystem.Utilities                        import Utils
#
#__RCSID__ = '$Id: $'
#
#class AlarmAction( BaseAlarmAction ):
#  '''
#  Extended AlarmAction for LHCbDIRAC, adding 
#  '''
#  pass  
  
# Commented out, the following method does not work. ShiftDBAgent is doing its job.
  
#  def get_shiftdb_users( self ):
#    '''
#    Gets Production shifter email from ShiftDB.
#    '''
#    try: 
#      url = urllib2.urlopen("http://lbshiftdb.cern.ch/shiftdb_report.php", timeout = 60)
#    except urllib2.URLError:
#      print 'Error opening: http://lbshiftdb.cern.ch/shiftdb_report.php'
#      return []   
#    lines = [l.split('|')[1:-1] for l in  url.readlines()]
#    lines = [ {'Date': e[0].strip(), 'Function': e[1].strip(), 'Phone':e[2].strip(), 'Morning':e[3].strip(),
#               'Evening':e[4].strip(), 'Night':e[5].strip() } for e in lines[1:]]
#
#    lines = [ e for e in lines if e['Function'] == "Grid Expert" or e['Function'] == "Production" ]
#    lines = [ (e["Morning"], e["Evening"], e["Night"]) for e in lines ]
#    lines = [ e for e in Utils.list_flatten(lines) if e]
#    lines = [ Utils.unpack(self.rmClient.getUserRegistryCache(name = e))[0][0] for e in lines ]
#    return [ {'Users': lines, 'Notifications': ["Mail"]} ] # Only mail notification since others are not working

# Disabled temporarily
#  def _getUsersToNotify( self ):
#    return BaseAlarmAction._getUsersToNotify( self ) + self.get_shiftdb_users()

################################################################################
#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF