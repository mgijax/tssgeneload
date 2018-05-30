#!/usr/local/bin/python

#
#  Purpose:
#
#	Run QC checks and generate QC report output file
#
#  Usage:
#
#      tssgeneQC.py  filename
#
#      where:
#          filename = path to the input file
#
#  Env Vars:
#
#      The following environment variables are set by the configuration
#      files that are sourced by the wrapper script:
#
#          QC_RPT
#	   
#  Inputs:
#
#       A tab-delimited file in the format:
#               field 1 : TSS ID (MGI)
#               field 2 : TSS Symbol
#               field 3 : Gene ID (MGI)
#               field 4 : Gene Symbol
#
#  Outputs:
#
#      - QC report (${QC_RPT})
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal initialization error occurred
#      2:  Non-fatal QC errors detected in the input files
#      3:  Fatal QC errors detected in the input file
#      4:  Warning QC
#
#  Assumes:
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Run the QC checks.
#      5) Close input/output files.
#
#  Notes:  None
#

import sys
import os
import string
import db

USAGE = 'Usage: tssgeneQC.py  inputFile'

# Report file name
qcRptFile = os.environ['QC_RPT']

# minimum number of lines expected in the input file
minLines = int(os.environ['MIN_LINES'])

# {tssID:key, ...}
tssLookup = {}

# {markerID:key, ...}
markerLookup = {}

# error list
errorList = []

# Counts reported when no fatal errors
counter = 0

# flags for errors
hasError = 0

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable
#
def checkArgs ():
    global inputFile

    if len(sys.argv) != 2:
        print USAGE
        sys.exit(1)

    inputFile = sys.argv[1]

    return

# end checkArgs() -------------------------------------

#
# Purpose: Perform initialization steps.
# Returns: Nothing
# Assumes: Nothing
# Effects: opens files
#
def init ():

    global tssLookup, markerLookup

    openFiles()
   
    # lookup of Tss symbols
    results = db.sql('''select a.accid, m.symbol, m.chromosome
	from MRK_Marker m, ACC_Accession a
	where m._Organism_key = 1
	and m._Marker_Status_key in (1)
	and m.name like 'transcription start site region %'
	and m._Marker_key = a._Object_key
	and a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.preferred = 1''', 'auto')
 
    for r in results:
        tssId = string.lower(r['accid'])
	symbol = string.lower(r['symbol'])
	tssLookup[tssId] = (symbol, r['chromosome'])

    # load lookup of Gene symbols
    results = db.sql('''select a.accid, m.symbol, m.chromosome
	from MRK_Marker m, ACC_Accession a
	where m._Organism_key = 1
	and m._Marker_Status_key in (1)
	and m.name not like 'transcription start site region %'
	and m._Marker_key = a._Object_key
	and a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.preferred = 1''', 'auto')

    for r in results:
	markerId = string.lower(r['accid'])
        symbol = string.lower(r['symbol'])
	markerLookup[markerId] = (symbol, r['chromosome'])

    return

# end init() -------------------------------------

# Purpose: Open input and output files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
#
def openFiles ():

    global fpInfile, fpQcRpt

    # curator input file
    try:
        fpInfile = open(inputFile, 'r')
    except:
        print 'Cannot open input file: %s' % inputFile
        sys.exit(1)
    
    # QC report file
    try:
        fpQcRpt = open(qcRptFile, 'w')
    except:
        print 'Cannot open report file: %s' % qcRptFile
        sys.exit(1)

    return

# end openFiles() -------------------------------------

#
# Purpose: run the QC checks
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variables, write report to file system
#
def runQcChecks ():

    global hasError, counter

    lineNum = 0

    for line in fpInfile.readlines():

	lineNum += 1
	
	# for reporting only
	lineStripped = string.strip(line)

	tokens = string.split(line, '\t')

	if len(tokens) < 4:
	    errorList.append('Less than 4 fields\n')
	    errorList.append('Line %s: %s\n' % (lineNum, lineStripped))
	    hasError = 1
	    continue

	tssId = string.strip(tokens[0])
	tssSymbol = string.strip(tokens[1])
	markerId = string.strip(tokens[2])
	markerSymbol = string.strip(tokens[3])

	if tssId == '' or tssSymbol == '' or markerId == '' or markerSymbol == '':
	    errorList.append('Missing field\n')
	    errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
	    hasError = 1
	    continue

	if string.lower(tssId) not in tssLookup:
	    errorList.append('Invalid TSS ID\n')
	    errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
	    hasError = 1
	else:
	    if not tssLookup[string.lower(tssId)][0] == string.lower(tssSymbol):
	        errorList.append('TSS ID does not match TSS Symbol\n')
	        errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
		hasError = 1

	if string.lower(markerId) not in markerLookup:
	    errorList.append('Invalid Gene ID\n')
	    errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
            hasError = 1
	else:
	    if not markerLookup[string.lower(markerId)][0] == string.lower(markerSymbol):
	        errorList.append('Gene ID does not match Gene Symbol\n')
	        errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
		hasError = 1

	if string.lower(tssId) in tssLookup and string.lower(markerId) in markerLookup:
	    if not tssLookup[string.lower(tssId)][1] == markerLookup[string.lower(markerId)][1]:
	        errorList.append('TSS Chromosome does not match Marker Chromosome\n')
		errorList.append('TSS Chromosome : ' + tssLookup[string.lower(tssId)][1] + '\n')
		errorList.append('Marker Chromosome : ' + markerLookup[string.lower(markerId)][1] + '\n')
	        errorList.append('Line %s: "%s"\n\n' % (lineNum, lineStripped))
		hasError = 1

	if hasError:
	    continue

	counter +=1

    #
    # Report any fatal errors and exit - if found in published file, the load will not run
    #

    if lineNum < minLines:
	fpQcRpt.write('\nInput file has < %s lines indicating an incomplete file.\n' % (minLines))
	fpQcRpt.write('total input lines: %s.\n' % (lineNum))
	fpQcRpt.write('No other QC checking will be done until this is fixed.\n')
	closeFiles()
	sys.exit(3)

    if hasError:

	fpQcRpt.write('\nThe following errors must be fixed before publishing; if present, the load will not run\n\n')
	    
	for line in errorList:
	    fpQcRpt.write(line)
	fpQcRpt.write('\n')

	closeFiles()
        sys.exit(3)

    print 'total relationships to be loaded: %s\n' % (counter)

    return

# end runQcChecks() -------------------------------------
	
#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
#
def closeFiles ():

    global fpInfile, fpQcRpt

    fpInfile.close()
    fpQcRpt.close()

    return

# end closeFiles() -------------------------------------

#
# Main
#

print 'running TSS-to-Gene sanity checks...'

checkArgs()
init()
runQcChecks()
closeFiles()
sys.exit(0)

