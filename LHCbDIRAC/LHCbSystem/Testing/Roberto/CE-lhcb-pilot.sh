#!/bin/bash

i=$1
echo "nodename=$i"
voms-proxy-info --all
lcg-cp -t 20 gsiftp://$i/etc/grid-security/groupmapfile file:///tmp/$i.group 
if [ "$?" == "0" ]; then 
	cat /tmp/$i.group |grep '/lhcb/Role=pilot'
	entry=`cat /tmp/$i.group |grep '/lhcb/Role=pilot'|wc -l`
	rm /tmp/$i.group
        if [ $entry -ge 2 ]; then
		echo "Role=pilot fully supported for LHCb at CE $i"
	#	echo "SAMOUT=10"
		pub=`lcg-tags --vo lhcb --ce $i --list | grep VO-lhcb-pilot |wc -l `
		if [ "$pub" == "0" ]; then
		    echo "Now publishing the tag using the command lcg-tags"
		    lcg-tags --vo lhcb --ce $i --add --tags VO-lhcb-pilot
		    if [ "$?" == 0 ]; then
			echo "Success publishing the tag on the IS"
			echo "SAMOUT=10"
		    else
			echo "Error publishing the tag on the CE: $i"
			echo " Run the command: lcg-tags --vo lhcb --ce $i --add --tags VO-lhcb-pilot"
			echo "SAMOUT=20"
		    fi
		else
		    echo "Tag already advertized in the IS"
		    echo "SAMOUT=10"
		fi
	else
	    if [ "$entry" == "1" ]; then
		echo "Role=pilot not fully supported for LHCb at $i. /etc/grid-security/groupmap file sounds slightly wrong"
		echo "SAMOUT=40"
	    else
		echo "Retrieved /etc/grid-security/groupmapfile but no pilot role setup for lhcb"
		echo "SAMOUT=50"
	    fi
	fi
else
    echo "Test failing retrieving the groupmap file from the remote CE"
    echo "SAMOUT=50"
fi
