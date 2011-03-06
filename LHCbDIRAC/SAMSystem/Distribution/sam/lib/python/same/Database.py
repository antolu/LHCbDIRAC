import SOAPpy
import re
from Config import config

class Database:
    "Class for accessing querying web service."

    def __init__(self, wsdl=config.get('webservices','query_wsdl'), debug=0):
        SOAPpy.Config.debug = debug
        self.service = SOAPpy.WSDL.Proxy(wsdl)

    class Map(SOAPpy.Types.anyType):
        "Hack for passing java.util.Map over SOAP."

        def __init__(self, dict):
            SOAPpy.Types.anyType.__init__(self, data=dict, typed=0)

        def _marshalData(self):
            "Encodes the dict like the Axis expects it (only RPC style)."
            res = ""
            for key, vals in self._data.items():
                res += "<item>\n"
                res += "  <key xsi:type=\"xsd:string\">" + str(key) + "</key>\n"
                res += "  <value SOAP-ENC:arrayType=\"xsd:string[" + \
                        str(len(vals)) + "]\" xsi:type=\"SOAP-ENC:Array\">\n"
                for val in vals:
                    res += "    <item>" + str(val) + "</item>\n"
                res += "   </value>\n"
                res += "</item>\n"
            return res

    def decode(str):
        "Unescapes special characters in the string."
        return str.replace("\\#","#").replace("\\|","|").replace("\\\\","\\")
    decode = staticmethod(decode) # because it doesn't need self

    def split(str, delim, esc):
        "Splits the string using given delimiter and escape characters."
        escaped = 0
        list = []
        start = 0

        for i in range(len(str)):
            if str[i] == delim and not escaped:
                list.append(str[start:i])
                start = i+1
            elif str[i] == esc:
                escaped = not escaped
            else:
                escaped = 0
        list.append(str[start:])
        return list
    split = staticmethod(split)

    def query(self, attrs, filter):
        "Queries the database using web service."

        hash = self.Map(filter)
        res = self.service.query(attrs, hash)

        if res == "":
            return []

        # split the result back to records and fields
        return [[self.decode(f) for f in self.split(n, "|", "\\")] 
                    for n in self.split(res, "#", "\\")]

    def getAvailableAttributes(self):
        "Returns the list of attributes available for querying."
        return self.service.getAvailableAttributes()

try:
    db = Database()
except:
    db = None

# vim:sw=4:ts=4:et
def getNodes(filter):
    attrs=['sitename','nodename','inmaintenance']
    nodes = db.query(attrs, filter)
    return nodes

def get(attrs,filter):
    try:
        tuples = db.query(attrs, filter)
    except SOAPpy.Types.faultType, e:
        m=re.search('Unknown attribute: (\S+)',e.faultstring)
        if m:
            bad_attr=m.groups()[0]
            raise Exception, 'Unknown filter attribute: '+bad_attr
        else:
            raise Exception, 'Unknown problem with query web service'
    except Exception, e:
        if e.args[0]=="'NoneType' object has no attribute 'query'":
            raise Exception, "Couldn't connect to the query web service!"
        else:
            raise Exception, 'Unknown problem with query web service'

    return tuples
