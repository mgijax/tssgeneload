#!/usr/local/bin/python
#
#  tssgeneload.py
###########################################################################
#
#  Purpose:
#
#      Validate input and create feature relationships bcp file
#
#  Usage:
#
#      tssgeneload.py 
#
#  Inputs:
#
#	1. load-ready TSS/Gene file tab-delimited in the following format
#	    1. TSS ID
#	    2. TSS Term
#	    3. Gene ID
#	    4. Gene Term
#
#	2. Configuration - see tssgeneload.config
#
#  Outputs:
#
#       1. MGI_Relationship.bcp
#
#  Exit Codes:
#
#      0:  Successful cotssletion
#      1:  An exception occurred
#      2:  bcp fails

#  Assumes:
#
#      1) QC checks have been run and all errors fixed
#	
#  Itsslementation:
#
#      This script will perform following steps:
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
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  03/30/2016  sc  Initial development
#
###########################################################################

itssort sys
itssort os
itssort string

itssort db
itssort mgi_utils

#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'
DATE = mgi_utils.date("%m/%d/%Y")
USAGE='tssgeneload.py'

#
#  GLOBALS
#

# input file
inFile = os.environ['INPUT_FILE_TOLOAD']

# output bcp files
bcpFile =   os.environ['RELATIONSHIP_BCP']
outputDir = os.environ['OUTPUTDIR']
relationshipFile = '%s/%s' % (outputDir, bcpFile)

# file descriptors
fpInFile = ''
fpRelationshipFile = ''

# The tssgene relationship category key 'tss_to_gene'
catKey = 1008

# the tssgene relationship term key 'tss_to_marker'
relKey = 17396910

# the tssgene qualifier key 'Not Specified'
qualKey = 11391898

# the tsso_marker evidence key 'Not Specified'
evidKey = 17396909

# the tssgene reference key 'J:229957'
refsKey = 209979

# tssgeneload user key
userKey = 1604

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key

# Lookups
tssHeaderLookup = {}
markerLookup = {}

# for bcp
bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
table = 'MGI_Relationship'
def checkArgs ():
    # Purpose: Validate the arguments to the script.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: exits if unexpected args found on the command line
    # Throws: Nothing

    if len(sys.argv) != 1:
        print USAGE
        sys.exit(1)
    return

# end checkArgs() -------------------------------

def init():
    # Purpose: create lookups, open files, create db connection, gets max
    #	keys from the db
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened,
    #  creates files in the file system, creates connection to a database

    global nextRelationshipKey, tssHeaderLookup, markerLookup

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    user = os.environ['MGD_DBUSER']
    passwordFileName = os.environ['MGD_DBPASSWORDFILE']
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)

    #
    # get next MGI_Relationship key
    #
    results = db.sql('''select max(_Relationship_key) + 1 as nextKey
	    from MGI_Relationship''', 'auto')
    if results[0]['nextKey'] is None:
	nextRelationshipKey = 1000
    else:
	nextRelationshipKey = results[0]['nextKey']

    #
    # create lookups
    #
    # lookup of TSS header terms
    results = db.sql('''select a.accid, t.term, t._Term_key
        from DAG_Node n, VOC_Term t, ACC_Accession a
        where n._Label_key = 3
        and n._Object_key = t._Term_key
        and t._Vocab_key = 5
        and t.isObsolete = 0
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 34
        and a.preferred = 1''', 'auto')

    for r in results:
        tssId = string.lower(r['accid'])
        termKey = r['_Term_key']
        tssHeaderLookup[tssId] = termKey

    # load lookup of Gene terms
    results = db.sql('''select a.accid, t.term, t._Term_key
        from VOC_Term t, ACC_Accession a
        where t._Vocab_key = 106
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 180''', 'auto')

    for r in results:
        markerId = string.lower(r['accid'])
        termKey = r['_Term_key']
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
        print 'Cannot open Feature relationships input file: %s' % inFile
        sys.exit(1)

    try:
        fpRelationshipFile = open(relationshipFile, 'w')
    except:
        print 'Cannot open Feature relationships bcp file: %s' % relationshipFile
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
    # Purpose: parses feature relationship file, does verification
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
	tokens = map(string.strip, string.split(line, TAB))
        tssId = string.lower(string.strip(tokens[0]))
	objKey1 = tssHeaderLookup[tssId]
        markerId = string.lower(string.strip(tokens[2]))
	objKey2 = markerLookup[markerId]

	# MGI_Relationship
	fpRelationshipFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
	    (nextRelationshipKey, TAB, catKey, TAB, objKey1, TAB, objKey2, TAB, relKey, TAB, qualKey, TAB, evidKey, TAB, refsKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

	nextRelationshipKey += 1
    
    return

# end createFiles() -------------------------------------

def doDeletes():
    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()
    db.useOneConnection(0)

    return

# end doDeletes() -------------------------------------

def doBcp():
    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, table, outputDir, bcpFile)
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(2)
    return

#####################
#
# Main
#
#####################

# check the arguments to this script, exit(1) if incorrect args
checkArgs()

# exit(1) if errors opening files
init()

# validate data and create load bcp files
createFiles()

# close all output files
closeFiles()

# delete existing relationships
doDeletes()

# exit(2) if bcp command fails
doBcp()
sys.exit(0)
