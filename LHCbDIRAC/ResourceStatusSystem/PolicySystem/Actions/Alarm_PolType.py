"""
AlarmPolType Actions
"""
import urllib
from DIRAC.ResourceStatusSystem.PolicySystem.Actions import Alarm_PolType
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient
from DIRAC.ResourceStatusSystem.Utilities import Utils

rmClient = ResourceManagementClient()

def get_shiftdb_users():
  url = urllib.urlopen("http://lbshiftdb.cern.ch/shiftdb_report.php")
  lines = [l.split('|')[1:-1] for l in  url.readlines()]
  lines = [ {'Date': e[0].strip(), 'Function': e[1].strip(), 'Phone':e[2].strip(), 'Morning':e[3].strip(),
             'Evening':e[4].strip(), 'Night':e[5].strip() } for e in lines[1:]]

  lines = [ e for e in lines if e['Function'] == "Grid Expert" or e['Function'] == "Production" ]
  lines = [ (e["Morning"], e["Evening"], e["Night"]) for e in lines ]
  lines = [ e for e in Utils.list_flatten(lines) if e]
  lines = [ Utils.unpack(rmClient.getUserRegistryCache(name = e))[0][0] for e in lines ]
  return [ {'Users': lines, 'Notifications': ["Mail"]} ] # Only mail notification since others are not working

class AlarmPolType(Alarm_PolType.AlarmPolType):
  def _getUsersToNotify(self):
    return Alarm_PolType.AlarmPolType._getUsersToNotify(self) + get_shiftdb_users()
