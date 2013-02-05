#Tue Nov 13 14:45:22 2012"""Automatically generated. DO NOT EDIT please"""
from GaudiKernel.Proxy.Configurable import *

class TutorialAlgorithm( ConfigurableAlgorithm ) :
  __slots__ = { 
    'OutputLevel' : 0, # int
    'Enable' : True, # bool
    'ErrorMax' : 1, # int
    'ErrorCount' : 0, # int
    'AuditAlgorithms' : False, # bool
    'AuditInitialize' : False, # bool
    'AuditReinitialize' : False, # bool
    'AuditRestart' : False, # bool
    'AuditExecute' : False, # bool
    'AuditFinalize' : False, # bool
    'AuditBeginRun' : False, # bool
    'AuditEndRun' : False, # bool
    'AuditStart' : False, # bool
    'AuditStop' : False, # bool
    'MonitorService' : 'MonitorSvc', # str
    'RegisterForContextService' : True, # bool
    'ErrorsPrint' : True, # bool
    'PropertiesPrint' : False, # bool
    'StatPrint' : True, # bool
    'TypePrint' : True, # bool
    'Context' : '', # str
    'RootInTES' : '', # str
    'RootOnTES' : '', # str
    'GlobalTimeOffset' : 0.00000000, # float
    'StatTableHeader' : ' |    Counter                                      |     #     |    sum     | mean/eff^* | rms/err^*  |     min     |     max     |', # str
    'RegularRowFormat' : ' | %|-48.48s|%|50t||%|10d| |%|11.7g| |%|#11.5g| |%|#11.5g| |%|#12.5g| |%|#12.5g| |', # str
    'EfficiencyRowFormat' : ' |*%|-48.48s|%|50t||%|10d| |%|11.5g| |(%|#9.6g| +- %|-#9.6g|)%%|   -------   |   -------   |', # str
    'UseEfficiencyRowFormat' : True, # bool
    'CounterList' : [ '.*' ], # list
    'StatEntityList' : [  ], # list
    'VetoObjects' : [  ], # list
    'RequireObjects' : [  ], # list
    'HistoProduce' : True, # bool
    'HistoPrint' : False, # bool
    'HistoCheckForNaN' : True, # bool
    'HistoSplitDir' : False, # bool
    'HistoOffSet' : 0, # int
    'HistoTopDir' : '', # str
    'HistoDir' : 'DefaultName', # str
    'FullDetail' : False, # bool
    'MonitorHistograms' : True, # bool
    'FormatFor1DHistoTable' : '| %2$-45.45s | %3$=7d |%8$11.5g | %10$-11.5g|%12$11.5g |%14$11.5g |', # str
    'ShortFormatFor1DHistoTable' : ' | %1$-25.25s %2%', # str
    'HeaderFor1DHistoTable' : '|   Title                                       |    #    |     Mean   |    RMS     |  Skewness  |  Kurtosis  |', # str
    'UseSequencialNumericAutoIDs' : False, # bool
    'AutoStringIDPurgeMap' : { '/' : '=SLASH=' }, # list
    'NTupleProduce' : True, # bool
    'NTuplePrint' : True, # bool
    'NTupleSplitDir' : False, # bool
    'NTupleOffSet' : 0, # int
    'NTupleLUN' : 'FILE1', # str
    'NTupleTopDir' : '', # str
    'NTupleDir' : 'DefaultName', # str
    'EvtColsProduce' : False, # bool
    'EvtColsPrint' : False, # bool
    'EvtColSplitDir' : False, # bool
    'EvtColOffSet' : 0, # int
    'EvtColLUN' : 'EVTCOL', # str
    'EvtColTopDir' : '', # str
    'EvtColDir' : 'DefaultName', # str
    'Output' : '', # str
    'Inputs' : [  ], # list
    'P2PVInputLocations' : [  ], # list
    'InputPrimaryVertices' : 'Rec/Vertex/Primary', # str
    'UseP2PVRelations' : True, # bool
    'WriteP2PVRelations' : True, # bool
    'ForceP2PVBuild' : True, # bool
    'IgnoreP2PVFromInputLocations' : False, # bool
    'ReFitPVs' : False, # bool
    'CheckOverlapTool' : 'CheckOverlap:PUBLIC', # str
    'VertexFitters' : {  }, # list
    'ParticleFilters' : {  }, # list
    'ParticleCombiners' : {  }, # list
    'ParticleReFitters' : {  }, # list
    'PVReFitters' : {  }, # list
    'DecayTreeFitters' : {  }, # list
    'MassFitters' : {  }, # list
    'LifetimeFitters' : {  }, # list
    'DirectionFitters' : {  }, # list
    'DistanceCalculators' : {  }, # list
    'PrimaryVertexRelator' : 'GenericParticle2PVRelator__p2PVWithIPChi2_OfflineDistanceCalculatorName_/P2PVWithIPChi2:PUBLIC', # str
    'DecayDescriptor' : '', # str
    'ForceOutput' : True, # bool
    'PreloadTools' : False, # bool
    'Particle' : 'Undefined', # str
    'MassWindow' : 10000.00000000, # float
    'MaxChi2' : 1000.00000000, # float
  }
  _propertyDocDct = { 
    'EvtColSplitDir' : """ Split long directory names into short pieces """,
    'NTupleDir' : """ Subdirectory for N-Tuples """,
    'StatTableHeader' : """ The header row for the output Stat-table """,
    'ErrorsPrint' : """ Print the statistics of errors/warnings/exceptions """,
    'ParticleReFitters' : """ Names of particle refitters """,
    'RegisterForContextService' : """ The flag to enforce the registration for Algorithm Context Service """,
    'NTupleLUN' : """ Logical File Unit for N-tuples """,
    'EvtColTopDir' : """ Top-level directory for Event Tag Collections """,
    'EvtColLUN' : """ Logical File Unit for Event Tag Collections """,
    'NTupleSplitDir' : """ Split long directory names into short pieces (suitable for HBOOK) """,
    'HeaderFor1DHistoTable' : """ The table header for printout of 1D histograms  """,
    'StatPrint' : """ Print the table of counters """,
    'CheckOverlapTool' : """ Name of Overlap Tool """,
    'VetoObjects' : """ Skip execute if one or more of these TES objects exists """,
    'HistoDir' : """ Histogram Directory """,
    'NTupleTopDir' : """ Top-level directory for N-Tuples """,
    'UseP2PVRelations' : """ Use P->PV relations internally. Forced to true if re-fitting PVs. Otherwise disabled for single PV events. Default: true. """,
    'P2PVInputLocations' : """ Particle -> PV Relations Input Locations """,
    'TypePrint' : """ Add the actal C++ component type into the messages """,
    'PVReFitters' : """ Names of Primary Vertex refitters """,
    'EvtColsPrint' : """ Print statistics for Event Tag Collections  """,
    'HistoTopDir' : """ Top level histogram directory (take care that it ends with '/') """,
    'AutoStringIDPurgeMap' : """ Map of strings to search and replace when using the title as the basis of automatically generated literal IDs """,
    'NTuplePrint' : """ Print N-tuple statistics """,
    'CounterList' : """ RegEx list, of simple integer counters for CounterSummary. """,
    'WriteP2PVRelations' : """ Write out P->PV relations table to TES. Default: true """,
    'ShortFormatFor1DHistoTable' : """ Format string for printout of 1D histograms """,
    'EfficiencyRowFormat' : """ The format for the regular row in the output Stat-table """,
    'VertexFitters' : """ Names of vertex fitters """,
    'ParticleFilters' : """ Names of ParticleFilters """,
    'EvtColsProduce' : """ General switch to enable/disable Event Tag Collections """,
    'RequireObjects' : """ Execute only if one or more of these TES objects exists """,
    'PreloadTools' : """ If true all tools are pre-loaded in initialize """,
    'LifetimeFitters' : """ The mapping of nick/name/type for ILifetimeFitter tools """,
    'ForceP2PVBuild' : """ Force construction of P->PV relations table. Default: false """,
    'EvtColOffSet' : """ Offset for numerical N-tuple ID """,
    'Output' : """ Output Location of Particles """,
    'PropertiesPrint' : """ Print the properties of the component  """,
    'ReFitPVs' : """ Refit PV """,
    'EvtColDir' : """ Subdirectory for Event Tag Collections """,
    'HistoSplitDir' : """ Split long directory names into short pieces (suitable for HBOOK) """,
    'HistoProduce' : """ Swith on/off the production of histograms  """,
    'UseEfficiencyRowFormat' : """ Use the special format for printout of efficiency counters """,
    'DecayTreeFitters' : """ The mapping of nick/name/type for IDecaytreeFitFit tools """,
    'DecayDescriptor' : """ Describes the decay """,
    'HistoPrint' : """ Switch on/off the printout of histograms at finalization """,
    'RegularRowFormat' : """ The format for the regular row in the output Stat-table """,
    'MassFitters' : """ The mapping of nick/name/type for IMassFit tools """,
    'FormatFor1DHistoTable' : """ Format string for printout of 1D histograms """,
    'NTupleOffSet' : """ Offset for numerical N-tuple ID """,
    'HistoOffSet' : """ OffSet for automatically assigned histogram numerical identifiers  """,
    'UseSequencialNumericAutoIDs' : """ Flag to allow users to switch back to the old style of creating numerical automatic IDs """,
    'StatEntityList' : """ RegEx list, of StatEntity counters for CounterSummary. """,
    'HistoCheckForNaN' : """ Swicth on/off the checks for NaN and Infinity for histogram fill """,
    'DirectionFitters' : """ The mapping of nick/name/type for IDirectionFit tools """,
    'NTupleProduce' : """ General switch to enable/disable N-tuples """,
    'DistanceCalculators' : """ The mapping of nick/name/type for IDistanceCalculator tools """,
    'ParticleCombiners' : """ Names of particle combiners, the basic tools for creation of composed particles """,
    'ForceOutput' : """ If true TES location is written """,
    'Inputs' : """ Input Locations forwarded of Particles """,
  }
  def __init__(self, name = Configurable.DefaultName, **kwargs):
      super(TutorialAlgorithm, self).__init__(name)
      for n,v in kwargs.items():
         setattr(self, n, v)
  def getDlls( self ):
      return 'AnalysisTutorial'
  def getType( self ):
      return 'TutorialAlgorithm'
  pass # class TutorialAlgorithm

class DummyAnalysisAlg( ConfigurableAlgorithm ) :
  __slots__ = { 
    'OutputLevel' : 0, # int
    'Enable' : True, # bool
    'ErrorMax' : 1, # int
    'ErrorCount' : 0, # int
    'AuditAlgorithms' : False, # bool
    'AuditInitialize' : False, # bool
    'AuditReinitialize' : False, # bool
    'AuditRestart' : False, # bool
    'AuditExecute' : False, # bool
    'AuditFinalize' : False, # bool
    'AuditBeginRun' : False, # bool
    'AuditEndRun' : False, # bool
    'AuditStart' : False, # bool
    'AuditStop' : False, # bool
    'MonitorService' : 'MonitorSvc', # str
    'RegisterForContextService' : True, # bool
    'ErrorsPrint' : True, # bool
    'PropertiesPrint' : False, # bool
    'StatPrint' : True, # bool
    'TypePrint' : True, # bool
    'Context' : '', # str
    'RootInTES' : '', # str
    'RootOnTES' : '', # str
    'GlobalTimeOffset' : 0.00000000, # float
    'StatTableHeader' : ' |    Counter                                      |     #     |    sum     | mean/eff^* | rms/err^*  |     min     |     max     |', # str
    'RegularRowFormat' : ' | %|-48.48s|%|50t||%|10d| |%|11.7g| |%|#11.5g| |%|#11.5g| |%|#12.5g| |%|#12.5g| |', # str
    'EfficiencyRowFormat' : ' |*%|-48.48s|%|50t||%|10d| |%|11.5g| |(%|#9.6g| +- %|-#9.6g|)%%|   -------   |   -------   |', # str
    'UseEfficiencyRowFormat' : True, # bool
    'CounterList' : [ '.*' ], # list
    'StatEntityList' : [  ], # list
    'VetoObjects' : [  ], # list
    'RequireObjects' : [  ], # list
    'HistoProduce' : True, # bool
    'HistoPrint' : False, # bool
    'HistoCheckForNaN' : True, # bool
    'HistoSplitDir' : False, # bool
    'HistoOffSet' : 0, # int
    'HistoTopDir' : '', # str
    'HistoDir' : 'DefaultName', # str
    'FullDetail' : False, # bool
    'MonitorHistograms' : True, # bool
    'FormatFor1DHistoTable' : '| %2$-45.45s | %3$=7d |%8$11.5g | %10$-11.5g|%12$11.5g |%14$11.5g |', # str
    'ShortFormatFor1DHistoTable' : ' | %1$-25.25s %2%', # str
    'HeaderFor1DHistoTable' : '|   Title                                       |    #    |     Mean   |    RMS     |  Skewness  |  Kurtosis  |', # str
    'UseSequencialNumericAutoIDs' : False, # bool
    'AutoStringIDPurgeMap' : { '/' : '=SLASH=' }, # list
    'NTupleProduce' : True, # bool
    'NTuplePrint' : True, # bool
    'NTupleSplitDir' : False, # bool
    'NTupleOffSet' : 0, # int
    'NTupleLUN' : 'FILE1', # str
    'NTupleTopDir' : '', # str
    'NTupleDir' : 'DefaultName', # str
    'EvtColsProduce' : False, # bool
    'EvtColsPrint' : False, # bool
    'EvtColSplitDir' : False, # bool
    'EvtColOffSet' : 0, # int
    'EvtColLUN' : 'EVTCOL', # str
    'EvtColTopDir' : '', # str
    'EvtColDir' : 'DefaultName', # str
    'Output' : '', # str
    'Inputs' : [  ], # list
    'P2PVInputLocations' : [  ], # list
    'InputPrimaryVertices' : 'Rec/Vertex/Primary', # str
    'UseP2PVRelations' : True, # bool
    'WriteP2PVRelations' : True, # bool
    'ForceP2PVBuild' : True, # bool
    'IgnoreP2PVFromInputLocations' : False, # bool
    'ReFitPVs' : False, # bool
    'CheckOverlapTool' : 'CheckOverlap:PUBLIC', # str
    'VertexFitters' : {  }, # list
    'ParticleFilters' : {  }, # list
    'ParticleCombiners' : {  }, # list
    'ParticleReFitters' : {  }, # list
    'PVReFitters' : {  }, # list
    'DecayTreeFitters' : {  }, # list
    'MassFitters' : {  }, # list
    'LifetimeFitters' : {  }, # list
    'DirectionFitters' : {  }, # list
    'DistanceCalculators' : {  }, # list
    'PrimaryVertexRelator' : 'GenericParticle2PVRelator__p2PVWithIPChi2_OfflineDistanceCalculatorName_/P2PVWithIPChi2:PUBLIC', # str
    'DecayDescriptor' : '', # str
    'ForceOutput' : True, # bool
    'PreloadTools' : False, # bool
  }
  _propertyDocDct = { 
    'EvtColSplitDir' : """ Split long directory names into short pieces """,
    'NTupleDir' : """ Subdirectory for N-Tuples """,
    'StatTableHeader' : """ The header row for the output Stat-table """,
    'ErrorsPrint' : """ Print the statistics of errors/warnings/exceptions """,
    'ParticleReFitters' : """ Names of particle refitters """,
    'RegisterForContextService' : """ The flag to enforce the registration for Algorithm Context Service """,
    'NTupleLUN' : """ Logical File Unit for N-tuples """,
    'EvtColTopDir' : """ Top-level directory for Event Tag Collections """,
    'EvtColLUN' : """ Logical File Unit for Event Tag Collections """,
    'NTupleSplitDir' : """ Split long directory names into short pieces (suitable for HBOOK) """,
    'HeaderFor1DHistoTable' : """ The table header for printout of 1D histograms  """,
    'StatPrint' : """ Print the table of counters """,
    'CheckOverlapTool' : """ Name of Overlap Tool """,
    'VetoObjects' : """ Skip execute if one or more of these TES objects exists """,
    'HistoDir' : """ Histogram Directory """,
    'NTupleTopDir' : """ Top-level directory for N-Tuples """,
    'UseP2PVRelations' : """ Use P->PV relations internally. Forced to true if re-fitting PVs. Otherwise disabled for single PV events. Default: true. """,
    'P2PVInputLocations' : """ Particle -> PV Relations Input Locations """,
    'TypePrint' : """ Add the actal C++ component type into the messages """,
    'PVReFitters' : """ Names of Primary Vertex refitters """,
    'EvtColsPrint' : """ Print statistics for Event Tag Collections  """,
    'HistoTopDir' : """ Top level histogram directory (take care that it ends with '/') """,
    'AutoStringIDPurgeMap' : """ Map of strings to search and replace when using the title as the basis of automatically generated literal IDs """,
    'NTuplePrint' : """ Print N-tuple statistics """,
    'CounterList' : """ RegEx list, of simple integer counters for CounterSummary. """,
    'WriteP2PVRelations' : """ Write out P->PV relations table to TES. Default: true """,
    'ShortFormatFor1DHistoTable' : """ Format string for printout of 1D histograms """,
    'EfficiencyRowFormat' : """ The format for the regular row in the output Stat-table """,
    'VertexFitters' : """ Names of vertex fitters """,
    'ParticleFilters' : """ Names of ParticleFilters """,
    'EvtColsProduce' : """ General switch to enable/disable Event Tag Collections """,
    'RequireObjects' : """ Execute only if one or more of these TES objects exists """,
    'PreloadTools' : """ If true all tools are pre-loaded in initialize """,
    'LifetimeFitters' : """ The mapping of nick/name/type for ILifetimeFitter tools """,
    'ForceP2PVBuild' : """ Force construction of P->PV relations table. Default: false """,
    'EvtColOffSet' : """ Offset for numerical N-tuple ID """,
    'Output' : """ Output Location of Particles """,
    'PropertiesPrint' : """ Print the properties of the component  """,
    'ReFitPVs' : """ Refit PV """,
    'EvtColDir' : """ Subdirectory for Event Tag Collections """,
    'HistoSplitDir' : """ Split long directory names into short pieces (suitable for HBOOK) """,
    'HistoProduce' : """ Swith on/off the production of histograms  """,
    'UseEfficiencyRowFormat' : """ Use the special format for printout of efficiency counters """,
    'DecayTreeFitters' : """ The mapping of nick/name/type for IDecaytreeFitFit tools """,
    'DecayDescriptor' : """ Describes the decay """,
    'HistoPrint' : """ Switch on/off the printout of histograms at finalization """,
    'RegularRowFormat' : """ The format for the regular row in the output Stat-table """,
    'MassFitters' : """ The mapping of nick/name/type for IMassFit tools """,
    'FormatFor1DHistoTable' : """ Format string for printout of 1D histograms """,
    'NTupleOffSet' : """ Offset for numerical N-tuple ID """,
    'HistoOffSet' : """ OffSet for automatically assigned histogram numerical identifiers  """,
    'UseSequencialNumericAutoIDs' : """ Flag to allow users to switch back to the old style of creating numerical automatic IDs """,
    'StatEntityList' : """ RegEx list, of StatEntity counters for CounterSummary. """,
    'HistoCheckForNaN' : """ Swicth on/off the checks for NaN and Infinity for histogram fill """,
    'DirectionFitters' : """ The mapping of nick/name/type for IDirectionFit tools """,
    'NTupleProduce' : """ General switch to enable/disable N-tuples """,
    'DistanceCalculators' : """ The mapping of nick/name/type for IDistanceCalculator tools """,
    'ParticleCombiners' : """ Names of particle combiners, the basic tools for creation of composed particles """,
    'ForceOutput' : """ If true TES location is written """,
    'Inputs' : """ Input Locations forwarded of Particles """,
  }
  def __init__(self, name = Configurable.DefaultName, **kwargs):
      super(DummyAnalysisAlg, self).__init__(name)
      for n,v in kwargs.items():
         setattr(self, n, v)
  def getDlls( self ):
      return 'AnalysisTutorial'
  def getType( self ):
      return 'DummyAnalysisAlg'
  pass # class DummyAnalysisAlg
