########################################################################
# $Id: ReplicaReader.py,v 1.1 2008/02/29 12:01:29 zmathe Exp $
########################################################################

"""

"""

from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.Replica      import Replica
from DIRAC.BookkeepingSystem.Agent.XMLReader.Replica.ReplicaParam import ReplicaParam
from DIRAC                                                        import gLogger, S_OK, S_ERROR

__RCSID__ = "$Id: ReplicaReader.py,v 1.1 2008/02/29 12:01:29 zmathe Exp $"

class ReplicaReader:
    
  #############################################################################  
  def __init__(self):
    pass
  
  #############################################################################  
  def readReplica(self, doc, filename):
    """
    
    """
    gLogger.info("Reading Replica from" + str(filename))                
    replica = Replica()
    replica.setFileName(filename) #full path
        
    replicaElements = doc.getElementsByTagName("Replica")
        
    for node in replicaElements:
      param = ReplicaParam()
            
      file = node.getAttributeNode('File')
      if file != None:
        param.setFile(file._get_value().encode('ascii'))
      else:
        gLogger.warn("Missing the <file> tag in replica xml file!")                
            
      name = node.getAttributeNode('Name')
      if name != None:
        param.setName(name._get_value().encode('ascii'))
      else:
        gLogger.warn("Missing the <name> tag in replica xml file!")
            
      location = node.getAttributeNode('Location')
      if location != None:
        param.setLocation(location._get_value().encode('ascii'))
      else:
        gLogger.warn("Missing the <location> tag in replica xml file!")

                
      se = node.getAttributeNode('SE')
      if se != None:
        param.setSE(se._get_value().encode('ascii'))
      else:
        gLogger.warn("Missing the <SE> tag in replica xml file!")
            
      action = node.getAttributeNode('Action')
      if action != None:
        param.setAction(action._get_value().encode('ascii'))
      else:
        gLogger.warn("Missing the <Action> tag in replica xml file!")
            
      replica.addParam(param)
      gLogger.info("Replica Reading fhinished succesefull!!")                
      return replica

###########################################################################  
        