#!/usr/local/bin/python
#
#  tssgeneQC.py
###########################################################################
#
#  Purpose:
#
#	This script will generate a QC report for the
#	    Tss-to-Gene Load file
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
# 	Tss-to-Gene input file
#	Columns:
#	1. Tss ID
#	2. Tss Symbol
#	3. Gene ID
#	4. Gene Symbol
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
###########################################################################

import sys
import os
import string
import re
import mgi_utils
import db

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'

USAGE = 'Usage: tssgeneQC.py  inputFile'

#
#  GLOBALS
#

# Report file name
qcRptFile = os.environ['QC_RPT']

# file with records to load 
inputFileToLoad = os.environ['INPUT_FILE_TOLOAD']

# minimum number of lines expected in the input file
minLines = int(os.environ['MIN_LINES'])

# {tssID:key, ...}
tssLookup = {}

# {markerID:key, ...}
markerLookup = {}

# input lines with missing data
missingDataList = []

# input lines with < 4 columns
missingColumnsList = []

# input lines with no Tss ID
missingTssIdList = []

# input Tss header IDs not in the database
invalidTssHeaderList = []

# input Tss header symbol does not match symbol in the database
invalidTssSymbolList = []

# input Gene IDs not in the database
invalidGene = []

# input Gene symbol does not match symbol in the database
invalidGene = []

# all passing QC (non-fatal, non-skip)
linesToLoadList = []

# used to determine duplicated lines in the input file
#{line:[line numbers], ...}
linesLookedAtDict = {}

# Counts reported when no fatal errors
loadCt = 0
skipCt = 0

# flags for errors
hasQcErrors = 0
hasFatalErrors = 0

#
# Purpose: Validate the arguments to the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: sets global variable
# Throws: Nothing
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
# Throws: Nothing
#
def init ():
    global markerLookup, tssLookup 

    openFiles()
   
    # load lookups 
    # lookup of Tss symbols
    results = db.sql('''select a.accid, m.symbol
	from MRK_Marker m, ACC_Accession a
	where m._Organism_key = 1
	and m._Marker_Status_key in (1,3)
	and m.name like 'transcription start site region %'
	and m._Marker_key = a._Object_key
	and a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.preferred = 1''', 'auto')
 
    for r in results:
        tssId = string.lower(r['accid'])
	symbol = string.lower(r['symbol'])
	tssLookup[tssId] = symbol

    # load lookup of Gene symbols
    results = db.sql('''select a.accid, m.symbol
	from MRK_Marker m, ACC_Accession a
	where m._Organism_key = 1
	and m._Marker_Status_key in (1,3)
	and m.name not like 'transcription start site region %'
	and m._Marker_key = a._Object_key
	and a._MGIType_key = 2
	and a._LogicalDB_key = 1
	and a.preferred = 1''', 'auto')

    for r in results:
	markerId = string.lower(r['accid'])
        symbol = string.lower(r['symbol'])
	markerLookup[markerId] = symbol

    return

# end init() -------------------------------------

# Purpose: Open input and output files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables.
# Throws: Nothing
#
def openFiles ():
    global fpInfile, fpToLoadFile, fpQcRpt

    # curator input file
    try:
        fpInfile = open(inputFile, 'r')
    except:
        print 'Cannot open input file: %s' % inputFile
        sys.exit(1)
    
    # all lines that pass QC
    try:
        fpToLoadFile = open(inputFileToLoad, 'w')
    except:
        print 'Cannot open input file: %s' % inputFileToLoad
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
# Throws: Nothing
#
def runQcChecks ():

    global hasQcErrors, hasFatalErrors, loadCt, skipCt
    global linesLookedAtDict

    lineNum = 1 # count header

    # throw away header
    header = fpInfile.readline()
    for line in fpInfile.readlines():
	lineNum += 1
	# don't strip the line or missing header symbol QC won't work; its the first column
	#print line
	#line = string.strip(line)
	
	# for reporting only
	lineStripped = string.strip(line)
	if linesLookedAtDict.has_key(line):
	    #print 'dup line'
	    linesLookedAtDict[line].append(str(lineNum))
	    hasFatalErrors = 1
	    continue
	else:
	    #print 'new line' 
	    linesLookedAtDict[line]= [str(lineNum)]
	tokens = string.split(line, TAB)
	#print 'count tokens: %s' % len(tokens)
	#print tokens
	#print '\n'
	#print 'lineNum: %s tokens: %s' % (lineNum, tokens)
	# skip blank lines
	if  len(tokens) == 1 and tokens[0] == '':
	    skipCt += 1
	    continue
	if len(tokens) < 4:
	    # if the first token is an Tss ID, then we are missing columns
	    #if string.find(tokens[0], 'Tss:') >= 0: 
	    #print 'missing columns line: %s' % lineNum
	    hasFatalErrors = 1
	    missingColumnsList.append('Line %s: %s%s' % (lineNum, lineStripped, CRT))
	    continue
	if tokens[0] == '':
	    #print 'missing Tss ID: %s' % lineNum
	    hasQcErrors = 1
	    missingTssIdList.append('Line %s: %s%s' % (lineNum, lineStripped, CRT))
	    continue

	tssId = string.strip(tokens[0])
	tssSymbol = string.strip(tokens[1])
	markerId = string.strip(tokens[2])
	# strip this token, there may or may not be a line break
	markerSymbol = string.strip(tokens[3])
	#print string.lower(tssId)
	#print string.lower(tssSymbol)
	#print string.lower(markerId)
	#print string.lower(markerSymbol)
	if tssId == '' or tssSymbol == '' or markerId == '' or markerSymbol == '':
	    missingDataList.append('Line %s: "%s"%s' % (lineNum, lineStripped, CRT))
	    #print 'hasFatalErrors missing data'
	    hasFatalErrors = 1
	    continue
	hasIdErrors = 0
	if not tssLookup.has_key(string.lower(tssId)):
	    invalidTssHeaderList.append('Line %s: "%s"%s' % (lineNum, lineStripped, CRT))
	    hasIdErrors = 1
	else:
	    if not tssLookup[string.lower(tssId)] == string.lower(tssSymbol):
		invalidTssSymbolList.append('Line %s: "%s"  In database: %s%s' % (lineNum, lineStripped, tssLookup[string.lower(tssId)], CRT))
		hasIdErrors = 1
	if not markerLookup.has_key(string.lower(markerId)):
	    invalidGene %s: "%s"%s' % (lineNum, lineStripped, CRT))
            hasIdErrors = 1
	else:
	    #print markerLookup[string.lower(markerId)]
	    #print 'should match:'
	    #print string.lower(markerSymbol)
	    if not markerLookup[string.lower(markerId)] == string.lower(markerSymbol):
		invalidGene %s: "%s"  In database: %s%s' % (lineNum, lineStripped, markerLookup[string.lower(markerId)], CRT))
		hasIdErrors = 1

	if hasIdErrors:
	    #print 'print hasFatalErrors hasIdErrors'
	    hasFatalErrors = 1
	    skipCt += 1
	    continue
	# If we get here, we have a good record, write it out to the load file
	loadCt +=1
	fpToLoadFile.write('%s%s' % (string.strip(line), CRT))

    #
    # Report any fatal errors and exit - if found in published file, the load 
    # will not run
    #

    if lineNum < minLines:
	fpQcRpt.write('\nInput file has < %s lines indicating an incomplete file. Total input lines: %s.\n No other QC checking will be done until this is fixed.\n' % (minLines, lineNum))
	closeFiles()
	sys.exit(3)
    if hasFatalErrors:
	fpQcRpt.write('\nThe following errors must be fixed before publishing; if present, the load will not run\n\n')
	    
	if len(missingColumnsList):
	    fpQcRpt.write('\nInput lines with missing columns:\n')
	    fpQcRpt.write('-----------------------------\n')
	    for line in missingColumnsList:
		fpQcRpt.write(line)
	    fpQcRpt.write('\n')

	if len(missingDataList):
	    fpQcRpt.write('\nInput lines with missing data:\n')
	    fpQcRpt.write('-----------------------------\n')
	    for line in missingDataList:
		fpQcRpt.write(line)
	    fpQcRpt.write('\n')

        if len(invalidTssHeaderList):
            fpQcRpt.write('\nInput lines with invalid Tss header IDs:\n')
            fpQcRpt.write('-----------------------------\n')
            for line in invalidTssHeaderList:
                fpQcRpt.write(line)
            fpQcRpt.write('\n')

	if len(invalidTssSymbolList):
	    fpQcRpt.write('\nInput lines where Tss symbol does not match symbol in the database:\n')
            fpQcRpt.write('-----------------------------\n')
            for line in invalidTssSymbolList:
                fpQcRpt.write(line)
            fpQcRpt.write('\n')

        if len(invalidGeneList):
            fpQcRpt.write('\nInput lines with invalid Gene IDs:\n')
            fpQcRpt.write('-----------------------------\n')
            for line in invalidGeneList:
                fpQcRpt.write(line)
            fpQcRpt.write('\n')

	if len(invalidGeneSymbolList):
            fpQcRpt.write('\nInput lines where Gene symbol does not match symbol in the database:\n')
            fpQcRpt.write('-----------------------------\n')
            for line in invalidGeneSymbolList:
                fpQcRpt.write(line)
            fpQcRpt.write('\n')
	for line in linesLookedAtDict:
	    headerWritten = 0
	    if len(linesLookedAtDict[line]) > 1:
		if not headerWritten:
		    fpQcRpt.write('\nDuplicate lines in the input file:\n')
		    fpQcRpt.write('-----------------------------\n')
		    headerWritten = 1
		lineNumString = string.join(linesLookedAtDict[line], ', ')
		fpQcRpt.write('Lines:%s%s%s' % (lineNumString, TAB, line))
		fpQcRpt.write('\n')
	closeFiles()
        sys.exit(3)
    #
    # Report any non-fatal errors
    #
    if hasQcErrors:
	fpQcRpt.write('\nThe following errors are non-fatal. These records will be skipped.\n\n')
	if len(missingTssIdList):
	    fpQcRpt.write('\nInput lines with missing Tss header ID:\n')
            fpQcRpt.write('-----------------------------\n')
            for line in missingTssIdList:
                fpQcRpt.write(line)
            fpQcRpt.write('\n')

    print '%sNumber with non-fatal QC errors, these will not be processed: %s' % (CRT, skipCt)
    
    print 'Total number that will be loaded: %s%s' % ( loadCt, CRT)
	#closeFiles()
	#sys.exit(2)

    return

# end runQcChecks() -------------------------------------
	
#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing
#
def closeFiles ():
    global fpInfile, fpToLoadFile, fpQcRpt
    fpInfile.close()
    fpToLoadFile.close()
    fpQcRpt.close()
    return

# end closeFiles() -------------------------------------

#
# Main
#
#print 'checkArgs'
checkArgs()
#print 'init'
init()
#print 'runQcChecks'
runQcChecks()
#print 'closeFiles'
closeFiles()
if hasQcErrors: 
    sys.exit(2)
else:
    sys.exit(0)

