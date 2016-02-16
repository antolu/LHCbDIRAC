"""
It stores the replica
"""
########################################################################
# $Id: ReplicaReader.py 54167 2012-07-03 14:25:07Z zmathe $
########################################################################


from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Replica.Replica      import Replica
from LHCbDIRAC.BookkeepingSystem.Service.XMLReader.Replica.ReplicaParam import ReplicaParam
from DIRAC                                                        import gLogger

__RCSID__ = "$Id: ReplicaReader.py 54167 2012-07-03 14:25:07Z zmathe $"

class ReplicaReader:
  """
  ReplicaReader class
  """
  #############################################################################
  def __init__(self):
    pass

  #############################################################################
  @staticmethod
  def readReplica(doc, filename):
    """reads the replica information"""
    gLogger.info("Reading Replica from" + str(filename))
    replica = Replica()
    replica.setFileName(filename) #full path

    replicaElements = doc.getElementsByTagName("Replica")

    for node in replicaElements:
      param = ReplicaParam()

      outputfile = node.getAttributeNode('File')
      if outputfile != None:
        param.setFile(outputfile.value.encode('ascii'))
      else:
        gLogger.warn("Missing the <file> tag in replica xml file!")

      name = node.getAttributeNode('Name')
      if name != None:
        param.setName(name.value.encode('ascii'))
      else:
        gLogger.warn("Missing the <name> tag in replica xml file!")

      location = node.getAttributeNode('Location')
      if location != None:
        param.setLocation(location.value.encode('ascii'))
      else:
        gLogger.warn("Missing the <location> tag in replica xml file!")


      se = node.getAttributeNode('SE')
      if se != None:
        param.setSE(se.value.encode('ascii'))
      else:
        gLogger.warn("Missing the <SE> tag in replica xml file!")

      action = node.getAttributeNode('Action')
      if action != None:
        param.setAction(action.value.encode('ascii'))
      else:
        gLogger.warn("Missing the <Action> tag in replica xml file!")

      replica.addParam(param)
      gLogger.info("Replica Reading fhinished succesefull!!")
      return replica

###########################################################################

