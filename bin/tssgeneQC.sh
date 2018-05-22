#!/bin/sh
#
#  tssgeneQC.sh
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the process that does QC
#      checks for the MP/HPO mapping load
#
#  Usage:
#
#      tssgeneQC.sh  filename  
#
#      where
#          filename = full path to the input file
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:
#	tssgene input file
#
#  Outputs:
#
#      - QC report for the input file.
#
#      - Log file (${QC_LOGFILE})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal initialization error occurred
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Validate & source the configuration files to establish the environment
#      3) Verify that the input file exists.
#      4) Initialize the log and report files.
#      5) Call tssgeneQC.py to generate the QC report.
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  05/22/2018  lec  Initial development
#
###########################################################################

if [ -z ${MGICONFIG} ]
then
        MGICONFIG=/usr/local/mgi/live/mgiconfig
        export MGICONFIG
fi

. ${MGICONFIG}/master.config.sh

CONFIG=${TSSGENELOAD}/tssgeneload.config

USAGE='Usage: tssgeneQC.sh  filename'

if [ $# -eq 1 ]
then
    INPUT_FILE=$1
else
    echo ${USAGE}; exit 1
fi
#
# Make sure the configuration file exists and source it.
#
if [ -f ${CONFIG} ]
then
    . ${CONFIG}
else
    echo "Missing configuration file: ${CONFIG}"
    exit 1
fi

#
# Make sure the input file exists (regular file or symbolic link).
#
if [ "`ls -L ${INPUT_FILE} 2>/dev/null`" = "" ]
then
    echo "Missing input file: ${INPUT_FILE}"
    exit 1
fi

#
# Initialize the log file.
#
LOG=${QC_LOGFILE}
rm -rf ${LOG}
touch ${LOG}

#
# Initialize the report files to make sure the current user can write to them.
#
rm -f ${QC_RPT}; >${QC_RPT}

#
# Run qc checks on the input file
#
echo "" >> ${LOG}
date >> ${LOG}
echo "Run QC checks on the input file" >> ${LOG}
${TSSGENELOAD}/bin/tssgeneQC.py ${INPUT_FILE}
STAT=$?
if [ ${STAT} -eq 0 ]
then
echo "No QC errors detected." | tee -a ${LOG}
echo "" | tee -a ${LOG}
fi

if [ ${STAT} -eq 1 ]
then
    echo "Fatal initialization error. See ${LOG}" | tee -a ${LOG}
    echo "" | tee -a ${LOG}
    exit ${STAT}
fi

if [ ${STAT} -eq 2 ]
then
    echo "Non-fatal QC errors detected. See ${QC_RPT}" | tee -a ${LOG}
    echo "" | tee -a ${LOG}
fi

if [ ${STAT} -eq 3 ]
then
    echo "Fatal QC errors detected. See ${QC_RPT}" | tee -a ${LOG}
    echo "" | tee -a ${LOG}
    exit ${STAT}
fi

echo "" >> ${LOG}
date >> ${LOG}
echo "Finished running QC checks on the input file" >> ${LOG}

exit 0
