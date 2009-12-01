SENDMAIL = "/usr/sbin/sendmail" 
import sys
import glob
import commands
import time
import os

units = {'B':(1.0/1024**4),'k':(1.0/(1024**3)),'M': ((1.0)/(1024**2)), 'G': ((1.0)/(1024)), 'T': (1.0)}
pathfile = 'monitor'
timeprefix = time.strftime("%Y-%m-%dT%H:%M:%S")

mail="lhcb-grid@cern.ch" #main recipient
pic_mail="lcg.support@pic.es"
cnaf_mail="t1-admin@cnaf.infn.it"
ral_mail="lcg-support@gridpp.rl.ac.uk"
in2p3_mail="grid.admin@cc.in2p3.fr"
sara_mail="grid.support@sara.nl"
gridka_mail="lcg-admin@lists.fzk.de"
cern_mail="roberto.santinelli@cern.ch joel.closier@cern.ch philippe.charpentier@cern.ch"


input_data= "/afs/cern.ch/user/s/santinel/public/pledgedResources_3.5.txt"

def configuration(input_data):
    pledged = {}
    newfile= open(input_data,"rt")
    for item in newfile:
        item = item.replace ( "\n", "" )
        item = item.strip()
        if item and item[0] != "#":
            values = item.split()
            if values[1] != "NA":
                pledged[values[0]] = int(values[1])
            else:
                pledged[values[0]] = values[1]
    return pledged

def xml(sito,free,total,timestamp,validity):
    if free < 4:
        if total != 0:
            availability=free*100/total
        else:
            availability=0
    else:
        availability=100

    xml = '''<?xml version='1.0' ?>
<serviceupdate xmlns='http://sls.cern.ch/SLS/XML/update'
xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'
xsi:schemaLocation='http://sls.cern.ch/SLS/XML/update
http://sls.cern.ch/SLS/XML/update.xsd'>
<id>''' +sito+'''</id>
<availability>'''+str(availability)+'''</availability>
<availabilitythresholds>
    <threshold level="available">15</threshold>
    <threshold level="affected">10</threshold>
    <threshold level="degraded">05</threshold>
</availabilitythresholds>
<availabilityinfo>Free='''+str(free)+''' Total='''+str(total)+'''</availabilityinfo>
<availabilitydesc>FreeSpace less than 4TB implies 0%-99% ;  FreeSpace greater than  4TB is always  100%</availabilitydesc>
<refreshperiod>PT27M</refreshperiod>
<validityduration>''' +validity+'''</validityduration>
<data>
<numericvalue name="Free space">'''+str(free)+'''</numericvalue>
<numericvalue name="Total space">'''+str(total)+'''</numericvalue>
<textvalue>Storage space for the specific space token</textvalue>
</data>
<timestamp>''' + timestamp +'''</timestamp>
</serviceupdate>
'''
    return xml

  
    
def converter(dimension):
  #print dimension
  if dimension == '0':
    return 0.0
  digit = float(dimension[:-1])
  #print digit
  unit = dimension[-1]
  #print unit
  return round(digit*units[unit],3)


def decode(input,info):
    #print input,info
    sito = input 
    lists = info.split()
    if 'Space' in lists:
	for i in xrange(len(lists)):
	    if 'totalSize' in lists[i]:
		total = converter(lists[i][10:]+'B')
		total = round(total*1.09951,3)
		print lists[i],total
	    if 'guaranteedSize' in lists[i]:
		guaranteed = converter(lists[i][15:]+'B')
		guaranteed = round(guaranteed*1.09951,3)
		print lists[i],guaranteed
	    if 'unusedSize' in lists[i]:
		unused = converter(lists[i][11:]+'B')
		unused = round(unused*1.09951,3)
		print lists[i],unused
            timequery=lists[-1]
            validityduration='PT13H'
    else:
	total = 0
	guaranteed = 0
	unused = 0
        timequery=time.strftime("%Y-%m-%dT%H:%M:%S")
        validityduration='PT0M'
        #2008-05-16T09:15:23
    outstring = xml(sito,unused,total,timequery,validityduration) 
    return outstring


def storage(input):
    print input
    token = input.split('_space')[0]
    id_name = token.replace("_","_")
    hostfile = input.replace("_space",".xml")
    domainfile = file(hostfile,'w')
    cmd = 'cat '+input
    err,info = commands.getstatusoutput(cmd)
    res = decode(id_name,info)
    domainfile.write(res)
    domainfile.close()
    
def alarm(input,pledge):
    token = input.split('_space')[0]
    pledged_resource= pledge[token]
    print "pledged resourses for token "+token+ " are: "+str(pledge[token])+"TB"
    id_name = token.replace("_","_")
    alarmfile=input.replace("_space",".alarm")
    cmd = 'cat '+input
    err,info = commands.getstatusoutput(cmd)
    sito = id_name
    lists = info.split()
    if 'Space' in lists:
	for i in xrange(len(lists)):
	    if 'totalSize' in lists[i]:
		total = converter(lists[i][10:]+'B')
		total = round(total*1.09951,3)
	    if 'guaranteedSize' in lists[i]:
		guaranteed = converter(lists[i][15:]+'B')
		guaranteed = round(guaranteed*1.09951,3)
	    if 'unusedSize' in lists[i]:
		unused = converter(lists[i][11:]+'B')
		unused = round(unused*1.09951,3)
            timequery=lists[-1]
            validityduration='PT13H'
    else:
	total = 0
	guaranteed = 0
	unused = 0
        timequery=time.strftime("%Y-%m-%dT%H:%M:%S")
        validityduration='PT0M'

    if unused < 4:
        if total != 0:
            if total < pledged_resource:
                print "@@@@@@ total: "+str(total)+ " pledge: "+str(pledged_resource)
                availability=unused*100/total
                availability=round(availability,2)
            else:
                availability=10000
        else:
            availability=1000
    else:
        availability=100

    availability2=100
    if total != 0:
        availability2=round(unused*100/total,2)

    slsUrl="http://sls.cern.ch/sls/service.php?id="
    pledgedUrl="https://twiki.cern.ch/twiki/bin/view/LHCb/StorageRequirements"
    mail2 = "unknown@unknown.com"
    
    if "PIC" in token:
        mail2 = pic_mail
    if "GRIDKA" in token:
        mail2 = gridka_mail
    if "RAL" in token:
        mail2 = ral_mail
    if "CERN" in token:
        mail2 = cern_mail
    if "CNAF" in token:
        mail2 = cnaf_mail
    if "IN2P3" in token:
        mail2 = in2p3_mail
    if "SARA" in token:
        mail2 = sara_mail

    print "-------------support mailing lists : "+mail2
    print "-------------availability: "+str(availability)
    if availability < 15:
        if os.path.isfile(alarmfile):
            print "****ALARM****:the availability for space token "+token+" is"+str(availability)
            print "the alarm file "+alarmfile+" already exists"
        else:
            print "****ALARM****:the availability for space token "+token+" is"+str(availability)
            print "the alarm file "+alarmfile+" does not exist. Creating it..."
            domainfile = file(alarmfile,'w')
            alarmmsg = '''
The availability for space token ''' +token+''' is ''' + str(availability)+ '''%
According to the policy defined in SLS the service results are now degraded (lower than 15%)
The free space for this space token is ''' +str(unused)+ '''TB over a total of '''+str(total)+'''TB provided.
LHCb pledged resources for this space token this year is: '''+str(pledged_resource)+'''TB
Please check the URL: <''' +slsUrl+token+'''> for more information about space consumption and the URL:<'''+pledgedUrl+'''> for more information about LHCb space requirements
'''

            domainfile.write(alarmmsg)
            domainfile.close()
            #sending now mail
            p = os.popen("%s -t" % SENDMAIL, "w")
            p.write("To: "+mail+" "+mail2+"\n")
            p.write("Subject: ***ALARM***: " +token+" almost full\n")
            p.write(alarmmsg)
            sts = p.close()
            if sts != 0:
                print "Sendmail exit status", sts
    else:
        if availability == 1000:
            print "The space token: "+token+" is not evaluable " 
        else:
            if availability == 10000 and availability2 < 15:
                if os.path.isfile(alarmfile):
                    print "****WARNING****:the unused space for space token "+token+" is: "+str(unused)+ "TB"
                    print "the warning file "+alarmfile+" already exists"
                else:
                    print "****WARNING****:the unused space for space token "+token+" is: "+str(unused)+ "TB"
                    print "the alarm file "+alarmfile+" does not exist. Creating it..."
                    domainfile = file(alarmfile,'w')
                    alarmmsg = '''The free space for space token ''' +token+''' is:  ''' + str(unused)+ '''TB which is below the alarm threshold. The total space allocated is ''' +str(total)+ '''TB greater than the pledged resources ('''+str(pledged_resource)+ '''TB). Please check the URL: <''' +slsUrl+token+'''> for more information about space consumption and the URL:<'''+pledgedUrl+'''> for more information about LHCb space requirements for this year and try to clean up some data.
                    '''
                    domainfile.write(alarmmsg)
                    domainfile.close()
#sending now mail
                    p = os.popen("%s -t" % SENDMAIL, "w")
                    p.write("To: "+mail+"\n")
                    p.write("Subject: ***WARNING***: " +token+" getting full but total allocated more than pledged \n")
                    p.write(alarmmsg)
                    sts = p.close()
                    if sts != 0:
                        print "Sendmail exit status", sts
            else:
                print "The space token: "+token+" is OK, no alarm needed"
                print "Removing eventual *.alarm files"
                if os.path.isfile(alarmfile):
                    os.remove(alarmfile)
                    print "****Resetting the alarm for space "+token

def main():
    dic = {}
    pledge = configuration(input_data)
    #print pledge['SARA-LHCb_RDST']
    cmd = 'ls *_space'
    err,tmp = commands.getstatusoutput(cmd)
    lines = tmp.split('\n')
    for line in lines:
	storage(line)
        toke = line.split('_space')[0]
        pledged=pledge[toke]
        if str(pledged) != "NA":
            alarm(line,pledge)
        else:
            print "The space token"+toke+" is T1D0 and there is not need for alarming"
		

if __name__ == '__main__':
    main()

