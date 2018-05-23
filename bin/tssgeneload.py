#!/usr/local/bin/python

#
#  Purpose:
#
#      To process TSS input file
#
#  Inputs:
#
#       A tab-delimited file in the format:
#		field 1 : TSS ID (MGI)
#		field 2 : TSS Symbol
#		field 3 : Gene ID (MGI)
#		field 4 : Gene Symbol
#
#  Outputs:
#
#	1 BCP file:
#	A pipe-delimited file:
#       	MGI_Relationship.bcp
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  bcp fails
#
#  Assumes:
#
#      1) QC checks have been run and all errors fixed
#	
#  Implementation:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Run the QC  checks
#      5) Run the load if QC checks pass
#      6) Close the input/output files.
#      7) Delete existing relationships
#      8) BCP in new relationships:
#
# History:
#
# lec	05/22/2018
#	- TR12734/GenFeVah/Fantom 5/TSS
#

import sys
import os
import string
import db
import mgi_utils

#db.setTrace()

# from configuration file
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
inFile = os.environ['INPUT_FILE_DEFAULT']
outputDir = os.environ['OUTPUTDIR']
bcpFile = 'MGI_Relationship.bcp'
relationshipFile = '%s/%s' % (outputDir, bcpFile)

cdate  = mgi_utils.date("%m/%d/%Y")
fpInFile = ''
fpRelationshipFile = ''

# The tssgene relationship category key 'tss_to_gene'
catKey = 1008

# the tssgene relationship term key 'transcription_start_site'
relKey = 41697543

# the tssgene qualifier key 'Not Specified'
qualKey = 11391898

# the tsso_marker evidence key 'Not Specified'
evidKey = 17396909

# the tssgene reference key 'J:208882'
refsKey = 209979

# tssgeneload user key
userKey = 1604

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key

# Lookups
tssLookup = {}
markerLookup = {}

def init():
    # Purpose: create lookups, open files, create db connection, gets max
    #	keys from the db
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened,
    #  creates files in the file system, creates connection to a database

    global nextRelationshipKey, tssLookup, markerLookup

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)

    #
    # get next MGI_Relationship key
    #
    results = db.sql('''select max(_Relationship_key) + 1 as nextKey from MGI_Relationship''', 'auto')
    if results[0]['nextKey'] is None:
	nextRelationshipKey = 1000
    else:
	nextRelationshipKey = results[0]['nextKey']

    #
    # create lookups
    #
    # lookup of TSS terms

    results = db.sql('''select a.accid, m._Marker_key
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
        termKey = r['_Marker_key']
        tssLookup[tssId] = termKey

    # load lookup of Gene terms
    results = db.sql('''select a.accid, m._Marker_key
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
        termKey = r['_Marker_key']
        markerLookup[markerId] = termKey

    return

# end init() -------------------------------

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened, 
    #  creates files in the file system

    global fpInFile, fpRelationshipFile

    try:
        fpInFile = open(inFile, 'r')
    except:
        print 'Cannot open relationships input file: %s' % inFile
        sys.exit(1)

    try:
        fpRelationshipFile = open(relationshipFile, 'w')
    except:
        print 'Cannot open relationships bcp file: %s' % relationshipFile
        sys.exit(1)

    return

# end openFiles() -------------------------------


def closeFiles ():
    # Purpose: Close all file descriptors
    # Returns: Nothing
    # Assumes: all file descriptors were initialized
    # Effects: Nothing
    # Throws: Nothing

    global fpInFile, fpRelationshipFile

    fpInFile.close()
    fpRelationshipFile.close()

    return

# end closeFiles() -------------------------------

def createFiles( ): 
    # Purpose: parses relationship file, does verification
    #  creates bcp files
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: sets global variables, writes to the file system
    # Throws: Nothing

    global nextRelationshipKey

    #
    # Iterate through the load ready input file
    #
    for line in fpInFile.readlines():
	tokens = map(string.strip, string.split(line, '\t'))
        tssId = string.lower(string.strip(tokens[0]))
	objKey1 = tssLookup[tssId]
        markerId = string.lower(string.strip(tokens[2]))
	objKey2 = markerLookup[markerId]

	# MGI_Relationship
	fpRelationshipFile.write('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % \
	    (nextRelationshipKey, catKey, objKey1, objKey2, relKey, qualKey, evidKey, refsKey, userKey, userKey, cdate, cdate))

	nextRelationshipKey += 1
    
    return

# end createFiles() -------------------------------------

def doDeletes():
    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()
    db.useOneConnection(0)
    return

# end doDeletes() -------------------------------------

def bcpFiles():

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'

    bcpCmd = '%s %s %s %s %s %s "|" "\\n" mgd' % \
            (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), 'MGI_Relationship', outputDir, bcpFile)

    rc = os.system(bcpCmd)

    if rc <> 0:
        closeFiles()
        sys.exit(2)

    return

# end bcpFiles() -------------------------------------

#
# Main
#

# exit(1) if errors opening files
init()

# validate data and create load bcp files
createFiles()

# close all output files
closeFiles()

# delete existing relationships
doDeletes()

# exit(2) if bcp command fails
bcpFiles()

sys.exit(0)

