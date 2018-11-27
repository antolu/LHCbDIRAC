""" Module holding MCStatsClient class
"""

from DIRAC.Core.Base.Client import Client


class MCStatsClient(Client):
  """ Client for MCStatsElasticDB. Can be specialized client by setting MCStatsClient().indexName
  """

  def __init__(self, **kwargs):
    """ simple constructor
    """

    super(MCStatsClient, self).__init__(**kwargs)
    self.setServer('ProductionManagement/MCStatsElasticDB')

    self.indexName = 'lhcb-mclogerrors'

  def set(self, typeName, data):
    """ set some data
    """
    self._getRPC().set(self.indexName, typeName, data)

  def get(self, jobID):
    """ get per Job ID
    """
    self._getRPC().get(self.indexName, jobID)

  def remove(self, jobID):
    """ remove data for JobID
    """
    self._getRPC().remove(self.indexName, jobID)
