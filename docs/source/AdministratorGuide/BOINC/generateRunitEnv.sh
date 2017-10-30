#!/bin/bash
function doInstall(){
  system=$1
  component=$2

  echo "Creating $system $component"

  mkdir -p $DIRAC/runit/$system/$component
  cat $DIRAC/runit/Framework/SystemAdministrator/run  | sed "s/Framework/$system/g" | sed "s/SystemAdministrator/$component/g" &> $DIRAC/runit/$system/$component/run
  chmod 755 $DIRAC/runit/$system/$component/run
  mkdir $DIRAC/runit/$system/$component/log
  cp $DIRAC/runit/Framework/SystemAdministrator/log/run $DIRAC/runit/$system/$component/log
  touch $DIRAC/etc/"$system"_"$component".cfg
  ln -s $DIRAC/runit/$system/$component/ $DIRAC/startup/"$system"_"$component"
}

doInstall DataManagement StorageElement
doInstall DataManagement StorageElementProxy
doInstall Framework ProxyManager
doInstall WorkloadManagement WMSSecureGW
doInstall WorkloadManagement SandboxStore
doInstall RequestManagement ReqManager
doInstall RequestManagement RequestProxy
doInstall RequestManagement RequestExecutingAgent
doInstall RequestManagement CleanReqDBAgent
