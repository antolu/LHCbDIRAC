#!/bin/bash

VOADMIN="santinel"
VOGROUP=`id -gn ${VOADMIN}`
GLOBUS_LOCATION=${GLOBUS_LOCATION:-/opt/globus}

export MYPROXY_SERVER=${MYPROXY_SERVER:-myproxy.cern.ch}

LCG_LOCATION=${LCG_LOCATION:-/opt/lcg}

RENEWER_DIR=~santinel/private

RENEWER_PROXY_REPOSITORY=$RENEWER_DIR/repository #directory where all proxies to be renewed are stored

if [ ! -d ${RENEWER_PROXY_REPOSITORY} ] ; then
	mkdir -p ${RENEWER_PROXY_REPOSITORY} || do_failure "Cannot create repository dir ${RENEWER_PROXY_REPOSITORY}"
	chown ${VOADMIN}.${VOGROUP} ${RENEWER_PROXY_REPOSITORY} 
	chmod 0700 ${RENEWER_PROXY_REPOSITORY}
fi

cat /afs/cern.ch/user/s/santinel/private/pas | voms-proxy-init -pwstdin -out $RENEWER_PROXY_REPOSITORY/santinel 


RENEWER_LOGDIR=$RENEWER_DIR/log #directory where all logs of operations are booked

if [ ! -d ${RENEWER_LOGDIR} ] ; then
	mkdir -p ${RENEWER_LOGDIR} || exit -1 
	chown ${VOADMIN}.${VOGROUP} ${RENEWER_LOGDIR}
	chmod 0700 ${RENEWER_LOGDIR_LOGDIR}
fi


rm ${RENEWER_LOGDIR}/events.log > /dev/null 2>&1

echo "####################################### " > ${RENEWER_LOGDIR}/events.log
echo "Welcome to the full myproxy server test " >> ${RENEWER_LOGDIR}/events.log
echo "Author: R.Santinelli                    " >> ${RENEWER_LOGDIR}/events.log
echo "####################################### " >> ${RENEWER_LOGDIR}/events.log
echo "                                        " >> ${RENEWER_LOGDIR}/events.log
echo " Now testing                            " >> ${RENEWER_LOGDIR}/events.log
echo "                                        " >> ${RENEWER_LOGDIR}/events.log

echo "NODENAME:$MYPROXY_SERVER" >> ${RENEWER_LOGDIR}/events.log
echo "`date +"%D %H:%M:%S"`: Running myproxy-init -s $MYPROXY_SERVER -d -n to upload a proxy on the myproxy server" >> ${RENEWER_LOGDIR}/events.log

cat ~santinel/private/pas | myproxy-init -s $MYPROXY_SERVER -d --stdin_pass -n >> ${RENEWER_LOGDIR}/events.log 2>&1

if [ $? != "0" ]; then
    echo "`date +"%D %H:%M:%S"` : Error. Problem uploading user proxy into $MYPROXY_SERVER server" >> ${RENEWER_LOGDIR}/events.log
    echo "SAMRESULT:50" >> ${RENEWER_LOGDIR}/events.log
    exit 1 
fi

echo "`date +"%D %H:%M:%S"`:  Dressing agent renewer credentials" >> ${RENEWER_LOGDIR}/events.log

export X509_USER_PROXY=$RENEWER_DIR/renewd-proxy.pem 

grid-proxy-info >> ${RENEWER_LOGDIR}/events.log


for CERT in `ls $RENEWER_PROXY_REPOSITORY`; do

    PROXY_DN="nothing"
    PROXY_DN=`${GLOBUS_LOCATION}/bin/grid-proxy-info -f ${RENEWER_PROXY_REPOSITORY}/${CERT} -subject` 

    if [ "$PROXY_DN" == "nothing" ]; then
	echo "`date +"%D %H:%M:%S"` : Error. Problem retrieving infor of  the proxy to be renewed" >> ${RENEWER_LOGDIR}/events.log
	echo "SAMRESULT:50" >> ${RENEWER_LOGDIR}/events.log
	exit -1 
    fi

    echo "`date +"%D %H:%M:%S"`: Going to renew this $PROXY_DN" >> ${RENEWER_LOGDIR}/events.log
    TMP_PROXY=`mktemp`
    ${GLOBUS_LOCATION}/bin/myproxy-get-delegation -a ${RENEWER_PROXY_REPOSITORY}/${CERT} -d -o $TMP_PROXY >> ${RENEWER_LOGDIR}/events.log 2>&1
#this is the core.....

    if [ $? -eq 0 ]; then
#in case of success (why not?) it overwrites old proxy with the renewed one. It will be valid for 12 hours
	mv $TMP_PROXY ${RENEWER_PROXY_REPOSITORY}/${CERT} 
	echo "`date +"%D %H:%M:%S"` : Proxy for DN \"${PROXY_DN}\" successfully renewed " >> ${RENEWER_LOGDIR}/events.log
	export X509_USER_PROXY=$RENEWER_PROXY_REPOSITORY/$CERT
	echo "`date +"%D %H:%M:%S"`: Destroying myproxy certificates" >> ${RENEWER_LOGDIR}/events.log
	myproxy-destroy -d >> ${RENEWER_LOGDIR}/events.log 2>&1
	if [ $? -eq 0 ]; then
	    echo "`date +"%D %H:%M:%S"`: Proxy certificate removed successfully from $MYPROXY_SERVER" >> ${RENEWER_LOGDIR}/events.log
	    echo "SAMRESULT:10" >> ${RENEWER_LOGDIR}/events.log
	    rm  $RENEWER_PROXY_REPOSITORY/$CERT
	    exit 0
	else
	    echo "`date +"%D %H:%M:%S"`: Proxy certificate *NOT* removed successfully from $MYPROXY_SERVER" >> ${RENEWER_LOGDIR}/events.log
	    echo "SAMRESULT:30" >> ${RENEWER_LOGDIR}/events.log
	    rm  $RENEWER_PROXY_REPOSITORY/$CERT
	fi
    else
	echo "`date +"%D %H:%M:%S"` : ERROR. Unable to renew proxy \"${PROXY_DN}\"" >> ${RENEWER_LOGDIR}/events.log
	export X509_USER_PROXY=$RENEWER_PROXY_REPOSITORY/$CERT
	echo "`date +"%D %H:%M:%S"`: Destroying myproxy certificates" >> ${RENEWER_LOGDIR}/events.log
	echo "SAMRESULT:60" >> ${RENEWER_LOGDIR}/events.log
	myproxy-destroy -d >> ${RENEWER_LOGDIR}/events.log 2>&1
    fi
done
