""" PluginScript module holds PluginScript class, which is an extension of DMScript class
"""

from DIRAC import S_OK, gLogger
from DIRAC.Core.Base import Script

from LHCbDIRAC.DataManagementSystem.Client.DMScript import DMScript
from DIRAC.DataManagementSystem.Utilities.DMSHelpers import resolveSEGroup

class Setter( object ):
  def __init__( self, obj, name ):
    self.name = name
    self.obj = obj
  def setOption( self, val ):
    if self.name.endswith( '=' ):
      try:
        self.obj.options[self.name[:-1]] = val if not self.name == "GroupSize=" else float( val )
      except ValueError as e:
        gLogger.exception( "Bad value for parameter", self.name, lException = e )
    else:
      self.obj.options[self.name] = True

    return S_OK()

class PluginScript( DMScript ):
  """ Scripts utilities class
  """

  def __init__( self ):
    super( PluginScript, self ).__init__()
    self.pluginParameters = {"Plugin=": "   Plugin name (mandatory)",
                             "Type=": "   Transformation type [Replication] (Removal automatic)",
                             "Parameters=": "   Additional plugin parameters ({<key>:<val>,[<key>:val>]}",
                             "RequestID=": "   Sets the request ID (default 0)"
                            }
    self.seParameters = ( "KeepSEs", "Archive1SEs", "Archive2SEs",
                     "MandatorySEs", "SecondarySEs", "DestinationSEs", "FromSEs",
                     "RAWStorageElements", "ProcessingStorageElements" )
    self.additionalParameters = {"GroupSize=": "   GroupSize parameter for merging (GB) or nb of files",
                                 "NumberOfReplicas=": "   Number of copies to create or to remove",
                                 "ProcessingPasses=": "   List of processing passes for the DeleteReplicasWhenProcessed plugin",
                                 "Period=": "   minimal period at which a plugin is executed (if instrumented)",
                                 "CleanTransformations": "   (only for DestroyDataset) clean transformations from the files being destroyed",
                                 'Debug': '   Sets a debug flag in the plugin',
                                 "UseRunDestination": "   for RAWReplication plugin, use the already defined run destination as storage"
    }
    self.setters = {}

  def registerPluginSwitches( self ):
    self.registerBKSwitches()

    for option in self.pluginParameters:
      self.setters[option] = Setter( self, option )
      Script.registerSwitch( '', option, self.pluginParameters[option], self.setters[option].setOption )

    for param in self.seParameters:
      param += '='
      self.setters[param] = Setter( self, param )
      Script.registerSwitch( "", param,
                             "   List of SEs for the corresponding parameter of the plugin",
                             self.setters[param].setOption )

    for option in self.additionalParameters:
      self.setters[option] = Setter( self, option )
      Script.registerSwitch( '', option, self.additionalParameters[option], self.setters[option].setOption )


  def getPluginParameters( self ):
    if 'Parameters' in self.options:
      params = eval( self.options['Parameters'] )
    else:
      params = {}
    # print self.options
    for key in set( self.options ) & set( param if not param.endswith( '=' ) else param[:-1] for param in self.additionalParameters ):
      params[key] = self.options[key]
    return params

  def getPluginSEParameters( self ):
    params = {}
    # print self.options
    for key in set( self.options ) & set( self.seParameters ):
      val = self.options[key]
      if val:
        val = resolveSEGroup( val.split( ',' ) )
      else:
        val = []
      params[key] = val
    return params
