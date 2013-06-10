"""Generic XMLParser used to convert a XML file into more
   pythonic style. A tree ox XMLNodes.
"""

__RCSID__ = "$Id$"

import xml.dom.minidom

################################################################################

class XMLNode:
  """XMLNodes represent XML elements. May have attributes. They have
     either children or value ( exclusive-or).
  """
  def __init__( self, name ):
    self.name = name
    self.attributes = {}
    self.children = None
    self.value = None

  def childrens( self, name ):
    """ return children """
    return [ child for child in self.children if child.name == name ]

  def __repr__( self ):
    return '< %s >' % self.name

################################################################################

class XMLTreeParser:
  """XMLTreeParser converts an XML file or a string into a tree of XMLNodes.
     It does not validate the XML.
  """
  def __init__( self ):
    self.tree = None

  def parse( self, xmlFile ):
    """ parse the XML """
    domXML = xml.dom.minidom.parse( xmlFile )
    self.__handleXML( domXML )
    return self.tree

  def parseString( self, xmlString ):
    """ parse the XML """
    domXML = xml.dom.minidom.parseString( xmlString )
    self.__handleXML( domXML )
    return self.tree

################################################################################
# AUXILIAR FUNCTIONS
################################################################################

  def __handleXML( self, xml ):
    self.tree = self.__handleElement( [ xml.firstChild ] )

  def __handleElement( self, elements ):
    """ treat each element """
    nodes = []

    for el in elements:

      if el.nodeType == el.TEXT_NODE:
        continue

      node = XMLNode( el.localName )
      node.attributes = self.__getAttributesDict( el )

      if len( el.childNodes ) == 1:
        node.value = self.__handleTextElement( el.childNodes[ 0 ] )
      else:
        node.children = self.__handleElement( el.childNodes )

      nodes.append( node )

    return nodes

  def __getAttributesDict( self, attributes ):
    """ get the attributes in a dictionnary """
    dictionary = {}
    for attr in attributes.attributes.values():
      dictionary[ attr.name.encode( 'ascii' ) ] = attr.value.encode( 'ascii' )
    return dictionary

  def __handleTextElement( self, textElement ):
    """ treat the Text element """
    return self.__getText( textElement )

  def __getText( self, node ):
    """ get the TEXT """
    data = ''
    if node.nodeType == node.TEXT_NODE:
      data = node.data.encode( 'ascii' )
    return data


"""
Utilies for XML Report
"""

def addChildNode( parentNode, tag, returnChildren, args ):
  '''
  Params
    :parentNode:
      node where the new node is going to be appended
    :tag:
      name if the XML element to be created
    :returnChildren:
      flag to return or not the children node, used to avoid unused variables
    :*args:
      possible attributes of the element
  '''

  allowedTags = [ 'Job', 'TypedParameter', 'InputFile', 'OutputFile',
                   'Parameter', 'Replica', 'SimulationCondition' ]

  def genJobDict( configName, configVersion, ldate, ltime ):
    return {
            "ConfigName"   : configName,
            "ConfigVersion": configVersion,
            "Date"         : ldate,
            "Time"         : ltime
           }
  def genTypedParameterDict( name, value, typeP = "Info" ):
    return {
            "Name"  : name,
            "Value" : value,
            "Type"  : typeP
            }
  def genInputFileDict( name ):
    return {
            "Name" : name
            }
  def genOutputFileDict( name, typeName, typeVersion ):
    return {
            "Name"        : name,
            "TypeName"    : typeName,
            "TypeVersion" : typeVersion
            }
  def genParameterDict( name, value ):
    return {
            "Name"  : name,
            "Value" : value
            }
  def genReplicaDict( name, location = "Web" ):
    return {
            "Name"     : name,
            "Location" : location
            }
  def genSimulationConditionDict():
    return {}

  if not tag in allowedTags:
    # We can also return S_ERROR, but this let's the job keep running.
    tagsDict = {}
  else:
    tagsDict = locals()[ 'gen%sDict' % tag ]( *args )

  childNode = xml.dom.minidom.Document().createElement( tag )
  for key, value in tagsDict.items():
    childNode.setAttribute( key, str( value ) )
  parentNode.appendChild( childNode )

  if returnChildren:
    return ( parentNode, childNode )
  return parentNode

################################################################################
# EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
