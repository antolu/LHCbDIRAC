from DIRAC.Core.Base.Client import Client


class MCStatsClient(Client):

  def __init__(self, **kwargs):

    Client.__init__(self, **kwargs)
    self.setServer('ProductionManagement/MCStatsElasticDB')
