#!/usr/bin/env groovy


properties([parameters([string(name: 'projectVersion', defaultValue: 'v9r3-pre16', description: 'The LHCbDIRAC version to install'),
                        string(name: 'DIRAC_test_repo', defaultValue: 'DIRACGrid', description: 'The DIRAC repo to use for getting the test code'),
                        string(name: 'DIRAC_test_branch', defaultValue: 'rel-v6r21', description: 'The DIRAC branch to use for getting the test code'),
                        string(name: 'LHCbDIRAC_test_repo', defaultValue: 'lhcb-dirac', description: 'The LHCbDIRAC repo to use for getting the test code'),
                        string(name: 'LHCbDIRAC_test_branch', defaultValue: 'devel', description: 'The LHCbDIRAC branch to use for getting the test code'),
                        string(name: 'LcgVer', defaultValue: 'v14r7', description: 'LCG version to install'),
                        string(name: 'modules', defaultValue: '', description: 'to override what is installed, e.g. with https://github.com/fstagni/DIRAC.git:::DIRAC:::someBranch,https://gitlab.cern.ch/fstagni/LHCbDIRAC.git:::LHCbDIRAC:::LbEnv')
                       ])])


node('dirac_ci_6') {
    // Clean workspace before doing anything
    deleteDir()

    withEnv([
        "DEBUG=True",
        "VO=LHCb",
        "DB_USER=Dirac",
        "DB_ROOTUSER=admin",
        "DB_HOST=dbod-dirac-ci.cern.ch",
        "DB_PORT=5501",
        "NoSQLDB_HOST=localhost",
        "NoSQLDB_PORT=9200",
        "DIRAC_DEPRECATED_FAIL=True"
        ]) {

        withCredentials([string(credentialsId: 'mysql-pass', variable: 'DB_PASSWORD'),
                         string(credentialsId: 'mysql-root-pass', variable: 'DB_ROOTPWD')]) {

            stage('GET') {

                echo "Here getting the code"

                dir("$WORKSPACE/TestCode"){
                    sh """
                        git clone https://github.com/${params.DIRAC_test_repo}/DIRAC.git
                        cd DIRAC
                        git checkout ${params.DIRAC_test_branch}
                        cd ..
                    """
                    sh """
                        git clone https://gitlab.cern.ch/${params.LHCbDIRAC_test_repo}/LHCbDIRAC.git
                        cd LHCbDIRAC
                        git checkout ${params.LHCbDIRAC_test_branch}
                        cd ..
                    """

                    echo "Got the test code"
                }
            }
            stage('SourceAndInstall') {

                echo "Sourcing and installing"
                sh """
                    source $WORKSPACE/TestCode/LHCbDIRAC/tests/Jenkins/lhcb_ci.sh
                    X509_CERT_DIR=$WORKSPACE/ServerInstallDIR/etc/grid-security/certificates/ fullInstallDIRAC
                """
                echo "**** Server INSTALLATION DONE ****"

                sh 'cp -r $TESTCODE/DIRAC/tests/ $SERVERINSTALLDIR/DIRAC/'
                sh 'cp -r $TESTCODE/LHCbDIRAC/tests/ $SERVERINSTALLDIR/LHCbDIRAC/'

                //stash includes: 'PilotInstallDIR/**', name: 'installation'

            }
            stage('Test') {
                echo "Starting the tests"

                try {
                    dir(env.WORKSPACE+"/ServerInstallDIR"){
                        sh 'echo HERE'
                    }
                } catch (e) {
                    // if any exception occurs, mark the build as failed
                    currentBuild.result = 'FAILURE'
                    throw e
                } finally {
                    // perform workspace cleanup only if the build have passed
                    // if the build has failed, the workspace will be kept
                    cleanWs cleanWhenFailure: false
                }
            }
        }
    }
}