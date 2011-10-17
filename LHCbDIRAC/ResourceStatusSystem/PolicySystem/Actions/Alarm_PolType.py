"""
AlarmPolType Actions
"""
import urllib
from DIRAC.ResourceStatusSystem.PolicySystem.Actions import Alarm_PolType
from DIRAC.ResourceStatusSystem.DB.ResourceManagementDB import ResourceManagementDB

rmDB = ResourceManagementDB()

def getUsersToNotifyShiftDB():
  url = urllib.urlopen("http://lbshiftdb.cern.ch/shiftdb_report.php")
  lines = [l.split('|')[1:-1] for l in  url.readlines()]
  lines = [ {'Date': e[0].strip(), 'Function': e[1].strip(), 'Phone':e[2].strip(), 'Morning':e[3].strip(),
             'Evening':e[4].strip(), 'Night':e[5].strip() } for e in lines[1:]]

  lines = [ e for e in lines if e['Function'] == "Grid Expert" or e['Function'] == "Production" ]

  return  [ rmDB.registryGetLoginFromName(e['Morning']) for e in lines ]

class AlarmPolType(Alarm_PolType.AlarmPolType):
  def getUsersToNotify(self):
    return Alarm_PolType.AlarmPolType.getUsersToNotify(self) + getUsersToNotifyShiftDB()
