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
# verify input file exists and is readable
#

if [ ! -r ${INPUT_FILE_DEFAULT} ]
then
    # set STAT for endJobStream.py
    STAT=1
    checkStatus ${STAT} "Cannot read from input file: ${INPUT_FILE_DEFAULT}"
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

# NOTE: keep this commented out until production release
#
# There should be a "lastrun" file in the input directory that was created
# the last time the load was run for this input file. If this file exists
# and is more recent than the input file, the load does not need to be run.
#
LASTRUN_FILE=${INPUTDIR}/lastrun
if [ -f ${LASTRUN_FILE} ]
then
    if test ${LASTRUN_FILE} -nt ${INPUT_FILE_DEFAULT}
    then

        echo "Input file has not been updated - skipping load" | tee -a ${LOG_PROC}
        # set STAT for shutdown
        STAT=0
        echo 'shutting down'
        shutDown
        exit 0
    fi
fi

echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run QC checks"  | tee -a ${LOG_DIAG}
${TSSGENELOAD}/bin/tssgeneQC.sh ${INPUT_FILE_DEFAULT}
STAT=$?

if [ ${STAT} -eq 1 ]
then
    checkStatus ${STAT} "An error occurred while generating the sanity/QC reports - See ${QC_LOGFILE}. tssgeneQC.sh"

    # run postload cleanup and email logs
    shutDown
fi

if [ ${STAT} -eq 3 ]
then
    checkStatus ${STAT} "Fatal QC errors detected. See ${QC_RPT}. tssgeneQC.sh"
    
    # run postload cleanup and email logs
    shutDown

fi

#
# run the load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run tssgeneload.py"  | tee -a ${LOG_DIAG}
${TSSGENELOAD}/bin/tssgeneload.py | tee -a ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "${TSSGENELOAD}/bin/tssgeneload.py"

#
# Archive a copy of the input file, adding a timestamp suffix.
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Archive input file" >> ${LOG_DIAG}
TIMESTATSS=`date '+%Y%m%d.%H%M'`
ARC_FILE=`basename ${INPUT_FILE_DEFAULT}`.${TIMESTATSS}
cp -p ${INPUT_FILE_DEFAULT} ${ARCHIVEDIR}/${ARC_FILE}

#
# Touch the "lastrun" file to note when the load was run.
#
if [ ${STAT} = 0 ]
then
    touch ${LASTRUN_FILE}
fi


# run postload cleanup and email logs

shutDown

