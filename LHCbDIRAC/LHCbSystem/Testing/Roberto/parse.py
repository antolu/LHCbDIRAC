import sys
import glob
import commands



units = {'B':(1.0/1024**4),'k':(1.0/(1024**3)),'M': ((1.0)/(1024**2)), 'G': ((1.0)/(1024)), 'T': (1.0)}
pathfile = 'monitor'

def converter(dimension):
  #print dimension
  if dimension == '0':
    return 0.0
  digit = float(dimension[:-1])
  print digit
  unit = dimension[-1]
  print unit
  return round(digit*units[unit],3)


def decode(info,token):
    lists = info.split()
    if 'Space' in lists:
	for i in xrange(len(lists)):
	    if 'totalSize' in lists[i]:
		total = converter(lists[i][10:]+'B')
		print lists[i],total
	    if 'guaranteedSize' in lists[i]:
		guaranteed = converter(lists[i][15:]+'B')
		print lists[i],guaranteed
	    if 'unusedSize' in lists[i]:
		unused = converter(lists[i][11:]+'B')
		print lists[i],unused
    else:
	total = 0
	guaranteed = 0
	unused = 0
    outstring = token + ' ' + str(total) + ' ' + str(guaranteed) + ' ' + str(unused) + '\n' 
    return outstring
    



def storage(input):
    hostfile = input+'_monitor'
    domainfile = file(hostfile,'w')
    cmd = 'cat '+input
    token = input.split('_space')[0]
    err,info = commands.getstatusoutput(cmd)
    res = decode(info,token)
    domainfile.write(res)
    domainfile.close()
    #cmd = 'mv *monitor '+pathfile
    #commands.getstatusoutput(cmd)
    


def main():
    dic = {}
    #cmd = 'ls IN2P3-CC_MCDISK_space'
    cmd = 'ls /afs/cern.ch/user/s/santinel/public/www/sls/storage_space/*_space'
    err,tmp = commands.getstatusoutput(cmd)
    lines = tmp.split('\n')
    for line in lines:
	print line
	storage(line)
	print ' '
		


if __name__ == '__main__':
    main()


















