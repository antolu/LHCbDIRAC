########################################################################
# $HeadURL$
# File :   XMLTreeParser.py
########################################################################

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
  """XMLTreeParser converts an XML file into a tree of XMLNodes.
     It does not validate the XML.
  """
  def __init__( self ):
    self.tree = None

  def parse( self, xmlFile ):
    """ parse the XML """
    domXML = xml.dom.minidom.parse( xmlFile )
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
    dictionnary = {}
    for attr in attributes.attributes.values():
      dictionnary[ attr.name.encode( 'ascii' ) ] = attr.value.encode( 'ascii' )
    return dictionnary

  def __handleTextElement( self, textElement ):
    """ treat the Text element """
    return self.__getText( textElement )

  def __getText( self, node ):
    """ get the TEXT """
    data = ''
    if node.nodeType == node.TEXT_NODE:
      data = node.data.encode( 'ascii' )
    return data

#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF#EOF
