#!/bin/sh

#
# This script is a wrapper around the process that loads 
# TSS/Gene relationships
#
#
#     tssgeneload.sh 
#

if [ -z ${MGICONFIG} ]
then
        MGICONFIG=/usr/local/mgi/live/mgiconfig
        export MGICONFIG
fi

. ${MGICONFIG}/master.config.sh

CONFIG=${TSSGENELOAD}/tssgeneload.config

#
# verify & source the configuration file
#

if [ ! -r ${CONFIG} ]
then
    echo "Cannot read configuration file: ${CONFIG}"
    exit 1
fi

. ${CONFIG}

#
#  Source the DLA library functions.
#

if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#

preload ${OUTPUTDIR}

#
# rm all files/dirs from OUTPUTDIR
#

cleanDir ${OUTPUTDIR}

#
# run the load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run tssgeneload.py"  | tee -a ${LOG_DIAG}
${PYTHON} ${TSSGENELOAD}/bin/tssgeneload.py | tee -a ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${TSSGENELOAD}/bin/tssgeneload.py"

# run postload cleanup and email logs

shutDown

