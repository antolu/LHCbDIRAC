##################################
# Description: creates records according to Grid Monitoring Probe Specification v0.91
# Author: Piotr Nyczyk(base code), Daniel Rodrigues
# Institute: CERN
# Created: January 08
# License:
# $Id: record.py,v 1.1 2008/09/21 11:20:25 kskaburs Exp $
##################################

from __future__ import generators
import sys
import exceptions

def load(input):
    record=Record()
    key=''
    value=''
    multiline=False
    line=True
    while line:
        line=input.readline()
        if line.strip()[:1]=='#':
            continue
        if not line:
            return None
        if line=='EOT\n':
            break
        elif ':' in line and not multiline:
            key,value=map(str.strip, line.split(':',1))
            key=key.lower()
            if key=="detailsdata":
                multiline=True
            if value=='EOT':
                multiline=True
                value=''
            else:
                if ( len( value ) > 0 and ( value[0].isalnum() or value[0] == '/' ) ) :
                    record[key]=value
                else :
                    raise RecordSyntaxException( 'Syntax error: Record \'%s\' for key \'%s\' not according to specification.' % (value , key) )
        elif multiline:  
            if len(value) > 0 and value[-1]!='\n':
                value+='\n'
            value+=line
        else:
            sys.stderr.write('Syntax error: '+line)
    if multiline:
        record[key]=value
    return record

def loadAll(input):
    record=True
    while record:
        record=load(input)
        if record:
            yield record
        
def loads(s):
     return load(map(lambda x: x+'\n',s.split('\n')))
 
class Record(dict):
    def dumps(obj):
        s=''
        multiline=None
        for k,v in obj.items():
            if k.lower() == 'detailsdata' :
                multiline=k,v
            elif '\n' in v:
                if multiline:
                    sys.stderr.write('Ignoring additional multiline values!')
                else:
                    multiline=k,v
            else:
                s+=k+': '+v+'\n'
        if multiline:
            k,v=multiline
            if v[-1]!='\n':
                v+='\n'
            s+=k+':'+v
        s+='EOT\n'
        return s


class RecordSyntaxException(exceptions.Exception) :

	def __init__(self, value):
		self.value = value
		
	def __str__(self):
                return repr(self.value)
