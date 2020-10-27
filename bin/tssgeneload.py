#  Purpose:
#
#      Find Genes that overlap TSS genes and
#       and load TSS/Gene relationships
#
#  Inputs:
#
#       TSS coordinates in the database
#	Gene coordinates in the database
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
#      3) Query DB for TSS Coordinates and Gene coordinates
#      4) Determine TSS/Gene overlap
#      5) Write out to relationship bcp
#      6) Delete existing relationships
#      7) BCP in new relationships:
#
# History:
#
# sc	10/15/2020
#	- TR13349/B39
#

import sys
import os
import string
import db
import mgi_utils

#db.setTrace()

CRT = '\n'
TAB = '\t'

# from configuration file
user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
outputDir = os.environ['OUTPUTDIR']
reportDir = os.environ['RPTDIR']
bcpFile = 'MGI_Relationship.bcp'
relationshipFile = '%s/%s' % (outputDir, bcpFile)

cdate  = mgi_utils.date("%m/%d/%Y")
fpRelationship = ''

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
# {mgiID:[chr, str, start, end], ...}
tssLookup = {}

# gene lookups 
# p1 :  plus strand, chr 1
# m1 :  minus strand, chr1
p1Lookup = {}
m1Lookup = {}
p2Lookup = {}
m2Lookup = {}
p3Lookup = {}
m3Lookup = {}
p4Lookup = {}
m4Lookup = {}
p5Lookup = {}
m5Lookup = {}
p6Lookup = {}
m6Lookup = {}
p7Lookup = {}
m7Lookup = {}
p8Lookup = {}
m8Lookup = {}
p9Lookup = {}
m9Lookup = {}
p10Lookup = {}
m10Lookup = {}
p11Lookup = {}
m11Lookup = {}
p12Lookup = {}
m12Lookup = {}
p13Lookup = {}
m13Lookup = {}
p14Lookup = {}
m14Lookup = {}
p15Lookup = {}
m15Lookup = {}
p16Lookup = {}
m16Lookup = {}
p17Lookup = {}
m17Lookup = {}
p18Lookup = {}
m18Lookup = {}
p19Lookup = {}
m19Lookup = {}
pXLookup = {}
mXLookup = {}
pYLookup = {}
mYLookup = {}
pXYLookup = {}
mXYLookup = {}
pUNLookup = {}
mUNLookup = {}
pMTLookup = {}
mMTLookup = {}

# Reporting:
# {tssKey: {[withinAttribsDict, 2KbAttribsDict]}
within2KbTie = {}

# # {tssKey: {[withinAttribsDict, 2KbAttribsDict]}
within2KbwithinClosest = {}

# {tssKey: {[withinAttribsDict, 2KbAttribsDict]}
within2Kb2KbClosest = {}

# {tssKey: withinAttribsDict
onlyWithin = {}

#{tssKey: 2kbAttribsDict}
only2Kb = {}

# #{tssKey: tssAttribs}
noGene = {}

#
# Purpose: create lookups, open files, create db connection, gets max keys from the db
# Returns: Nothing
# Assumes: Nothing
# Effects: Sets global variables, exits if a file can't be opened,
#
def init():

    global nextRelationshipKey, tssLookup, p1Lookup, m1Lookup, p21Lookup
    global m2Lookup, p3Lookup, m3Lookup, p4Lookup, m4Lookup, p5Lookup, m5Lookup
    global p6Lookup, m6Lookup, p7Lookup, m7Lookup, p8Lookup, m8Lookup, p9Lookup
    global m9Lookup, p10Lookup, m10Lookup, p11Lookup, m11Lookup, p12Lookup 
    global m12Lookup, p13Lookup, m13Lookup, p14Lookup, m14Lookup, p15Lookup 
    global m15Lookup, p16Lookup, m16Lookup,p17Lookup, m17Lookup, p18Lookup 
    global m18Lookup, p19Lookup, m19Lookup, pXLookup, mXLookup, pYLookup
    global mYLookup, pUNLookup, mUNLookup, mXYLookup, pXYLookup, pMTLookup
    global mMTLookup

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
    # lookup of TSS markers

    results = db.sql('''select a.accid, m.symbol, m._Marker_key, lc.chromosome, lc.strand,
            cast(lc.startCoordinate as int) as start, 
            cast(lc.endCoordinate as int) as end
        from MRK_Marker m, ACC_Accession a, MRK_Location_Cache lc
        where m._Organism_key = 1
        and m._Marker_Status_key in (1,3)
        and m.name like 'transcription start site region %'
        and m._Marker_key = a._Object_key
        and a._MGIType_key = 2
        and a._LogicalDB_key = 1
        and a.preferred = 1
        and m._Marker_key = lc._Marker_key
        and lc.startCoordinate is not null
        and lc.endCoordinate is not null''', 'auto')

    for r in results:
        tssId = str.lower(r['accid'])
        markerKey = r['_Marker_key']
        tssLookup[markerKey] = [r['chromosome'], r['strand'], int(r['start']), int(r['end']), r['accid'], r['symbol']]

    # load lookup of Gene terms - exclude null strand and feature type
    # heritable phenotypic marker
    results = db.sql('''select a.accid, m.symbol, m._Marker_key, lc.chromosome, lc.strand,
            cast(lc.startCoordinate as int) as start, cast(lc.endCoordinate as int) as end
        from MRK_Marker m, ACC_Accession a, MRK_Location_Cache lc,
            VOC_Annot v
        where m._Organism_key = 1 
        and m._Marker_Status_key in (1,3)
        and m._Marker_Type_key in (1, 7)
        and m.name not like 'transcription start site region %'
        and m._Marker_key = a._Object_key
        and a._MGIType_key = 2 
        and a._LogicalDB_key = 1 
        and a.preferred = 1
        and m._Marker_key = lc._Marker_key
        and lc.startCoordinate is not null
        and lc.endCoordinate is not null
        and lc.strand is not null
        and m._Marker_key = v._Object_key
        and v._AnnotType_key = 1011
        and v._Term_key != 6238170''', 'auto')

    for r in results:
        geneId = str.lower(r['accid'])
        markerKey = r['_Marker_key']
        strand = 'p'    # plus strand
        if r['strand'] == '-': # minus strand
             strand = 'm'
        prefix = '%s%s' % (strand, r['chromosome'])
        currentLookup = eval('%sLookup' % (prefix))
        currentLookup[markerKey] = [r['chromosome'], r['strand'], int(r['start']), int(r['end']), r['accid'], r['symbol']]
        #print(('%sLookup' % prefix))
        #print('%s %s' %(geneId, r['_Marker_key']))
        #print((currentLookup[geneId]))
    return

# end init() -------------------------------

# Purpose: Open input/output files.
# Returns: exit 1 if cannot open input or output file
# Assumes: Nothing
# Effects: sets global variables, exits if a file can't be opened
#
def openFiles ():

    global fpRelationship, fpWithin2KbTie, fpWithin2KbWithinClosest
    global fpWithin2Kb2KbClosest, fpOnlyWithin, fpOnly2Kb, fpNoGene

    try:
        fpRelationship = open(relationshipFile, 'w')
    except:
        print(('Cannot open relationships bcp file: %s' % relationshipFile))
        sys.exit(1)
    try:
        fpWithin2KbTie = open('%s/Within2KbTie.rpt' % reportDir, 'w')
    except:
        print(('Cannot open Within2KbTie file: %s' % relationshipFile))
        sys.exit(1)

    try:
        fpWithin2KbWithinClosest = open('%s/Within2KBwithinClosest.rpt' % reportDir, 'w')
    except:
        print(('Cannot open Within2KBwithinClosest file: %s' % relationshipFile))
        sys.exit(1)

    try:
        fpWithin2Kb2KbClosest = open('%s/Within2Kb2KbClosest.rpt' % reportDir, 'w')
    except:
        print(('Cannot open Within2KB2KbClosest file: %s' % relationshipFile))
        sys.exit(1)

    try:
        fpOnlyWithin = open('%s/OnlyWithin.rpt' % reportDir, 'w')
    except:
        print(('Cannot open OnlyWithin file: %s' % relationshipFile))
        sys.exit(1)

    try:
        fpOnly2Kb = open('%s/Only2Kb.rpt' % reportDir, 'w')
    except:
        print(('Cannot open Only2Kb file: %s' % relationshipFile))
        sys.exit(1)

    try:
        fpNoGene = open('%s/NoGene.rpt' % reportDir, 'w')
    except:
        print(('Cannot open NoGene file: %s' % relationshipFile))
        sys.exit(1)


    return

# end openFiles() -------------------------------

#
# Purpose: Close the files.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
#
def closeFiles ():

    global fpRelationship, fpWithin2KbTie, fpWithin2KbWithinClosest
    global fpWithin2Kb2KbClosest, fpOnlyWithin, fpOnly2Kb, fpNoGene

    fpRelationship.close()
    fpWithin2KbTie.close()
    fpWithin2KbWithinClosest.close()
    fpWithin2Kb2KbClosest.close() 
    fpOnlyWithin.close()
    fpOnly2Kb.close()
    fpNoGene.close()

    return

# end closeFiles() -------------------------------

# Purpose: Determines TSS/Gene overlaps, writes to relationship bcp
# Returns: Nothing
# Assumes: file descriptors have been initialized
# Effects: sets global variables, writes to the file system
#
def findRelationships( ): 

    global nextRelationshipKey

    #
    # Iterate through the TSS genes
    #
    print("len(tssLookup): %s" % len(tssLookup))
    for tMarkerKey in tssLookup:
        attribList = tssLookup[tMarkerKey]
        tChromosome = attribList[0]
        #if tChromosome != '2':
        #    continue
        tStrand = attribList[1]
        tStart = attribList[2]
        tEnd = attribList[3]
        tAccid = attribList[4]
        tSymbol = attribList[5]

        tMidPoint = int(tEnd-((tEnd-tStart+1)/2)) # cast to int for rounding
        #print('tMarkerKey: %s tAccid: %s tSymbol: %s tChromosome: %s tStrand: %s tStart: %s tEnd: %s tMidPoint: %s' % (tMarkerKey, tAccid, tSymbol, tChromosome, tStrand, tStart, tEnd, tMidPoint))

        strand = 'p'    # plus strand
        if tStrand == '-': # minus strand
             strand = 'm'
        prefix = '%s%s' % (strand, tChromosome)
        geneLookup = eval('%sLookup' % prefix)
        
        currentWithinGenes = {}
        current2KBGenes = {}
        for gMarkerKey in geneLookup:
            #if gMarkerKey == '84154':
            #    print('found Arfgef1')
            gChromosome = geneLookup[gMarkerKey][0]
            gStrand =  geneLookup[gMarkerKey][1]
            gStart = geneLookup[gMarkerKey][2]
            gEnd = geneLookup[gMarkerKey][3]
            gAccid = geneLookup[gMarkerKey][4]
            gSymbol = geneLookup[gMarkerKey][5]

            #
            # TSS midpoint within 2KB upstream
            #
            if gStrand == '+':
                startsite = gStart - tMidPoint
            else: # gStrand == '-'
                startsite = tMidPoint - gEnd
            if startsite >= 0 and startsite <= 2000:
                #print('saving tss midpoint within twoKBUp')
                current2KBGenes[gMarkerKey] = geneLookup[gMarkerKey]
                #print('gMarkerKey: %s gAccid: %s gSymbol: %s gChromosome: %s gStrand: %s gStart: %s gEnd: %s StartSite: %s' % (gMarkerKey, gAccid, gSymbol, gChromosome, gStrand, gStart, gEnd, startsite))

            #
            # TSS midpoint within the Gene
            #

            # debug
            #within = 0
        
            if gStrand == '+':
                if tMidPoint - gStart > 0 and tMidPoint <= gEnd:
                    currentWithinGenes[gMarkerKey] = geneLookup[gMarkerKey]
                    #within = 1
            else: # gStrand == '-'
                if gEnd - tMidPoint > 0 and tMidPoint >= gStart:
                    currentWithinGenes[gMarkerKey] = geneLookup[gMarkerKey]
                    #within = 1
            #if within == 1:
                #print ('saving tss migPoint within gene')
                #print('gMarkerKey: %s gAccid: %s gSymbol: %s gChromosome: %s gStrand: %s gStart: %s gEnd: %s StartSite neg strand: %s' % (gMarkerKey, gAccid, gSymbol, gChromosome, gStrand, gStart, gEnd, gEnd - tMidPoint))
      
        # Find the gene where start site is closest to the TSS midpoint
        # Do this separately for within and 2KB
        #print('look for currentWithinClosestStart and currentWithinBestGene')
        currentWithinClosestStart = None
        currentWithinBestGene = ''
        for mKey in currentWithinGenes:
           gStrand = currentWithinGenes[mKey][1]
           gStart = currentWithinGenes[mKey][2]
           gEnd = currentWithinGenes[mKey][3]
           if gStrand == '+':
               ss = tMidPoint - gStart
           else: # gStrand is '-'
               ss = gEnd - tMidPoint
           #print('Part 2 within ss: %s' % ss)
           if currentWithinClosestStart == None:
               currentWithinClosestStart = ss 
               currentWithinBestGene = mKey
               continue
           if ss <= currentWithinClosestStart:
               currentWithinClosestStart = ss 
               currentWithinBestGene = mKey
           #print('currentWithinClosestStart:%s currentWithinBestGene: %s' % (currentWithinClosestStart, currentWithinBestGene))

        #print('look for current2KBClosestStart and current2KBBestGene')
        current2KBClosestStart = None
        current2KBBestGene = ''
        for mKey in current2KBGenes:
           gStrand = current2KBGenes[mKey][1]
           gStart = current2KBGenes[mKey][2]
           gEnd = current2KBGenes[mKey][3]
           if gStrand == '+':
               ss = gStart - tMidPoint
           else: # gStrand == '-'
               ss = tMidPoint - gEnd

           #print('Part 2 mKey: %s gStrand: %s 2KB ss: %s' % (mKey, gStrand, ss))
           if current2KBClosestStart == None:
               current2KBClosestStart = ss
               current2KBBestGene = mKey
               continue
           if ss <= current2KBClosestStart:
               current2KBClosestStart = ss
               current2KBBestGene = mKey
           #print('current2KBClosestStart:%s current2KBBestGene: %s' % (current2KBClosestStart, current2KBBestGene))
        #print('now choose the gene to create the relationship with')
        #print ('currentWithinBestGene: %s currentWithinClosestStart: %s current2KBBestGene: %s current2KBClosestStart: %s' % (currentWithinBestGene, currentWithinClosestStart, current2KBBestGene, current2KBClosestStart))
        geneKeyToUse = ''

        # if no gene, skip
        if currentWithinBestGene == '' and current2KBBestGene == '':
            #print('F! No gene for tssMarker: %s attributes: %s' % (tMarkerKey, attribList))
            noGene[tMarkerKey] = (tssLookup[tMarkerKey])
            continue
        if currentWithinBestGene != '' and current2KBBestGene != '':
            if currentWithinClosestStart == current2KBClosestStart:
                #print('A! currentWithinClosestStart: %s == current2KBClosestStart: %s, pick currentWithinBestGene: %s' % (currentWithinClosestStart,  current2KBClosestStart, currentWithinBestGene))
                within2KbTie[tMarkerKey] = [currentWithinGenes]
                within2KbTie[tMarkerKey].append(current2KBGenes)
                geneKeyToUse = currentWithinBestGene
            elif currentWithinClosestStart < current2KBClosestStart:
                #print('B! currentWithinClosestStart:%s < current2KBClosestStart: %s, pick currentWithinBestGene: %s' % (currentWithinClosestStart, current2KBClosestStart, currentWithinBestGene))
                within2KbwithinClosest[tMarkerKey] = [currentWithinGenes]
                within2KbwithinClosest[tMarkerKey].append(current2KBGenes)
                geneKeyToUse = currentWithinBestGene
            else:
                #print('C! current2KBClosestStart:%s < currentWithinClosestStart: %s, pick currentsKBBestGene: %s' % (current2KBClosestStart, currentWithinClosestStart, current2KBBestGene))
                within2Kb2KbClosest[tMarkerKey] = [currentWithinGenes]
                within2Kb2KbClosest[tMarkerKey].append(current2KBGenes)
                geneKeyToUse = current2KBBestGene
        elif currentWithinBestGene != '':
            #print('D! pick currentWithinBestGene: %s' % currentWithinBestGene)
            onlyWithin[tMarkerKey] = currentWithinGenes
            geneKeyToUse = currentWithinBestGene
        else:
            #print('E! pick current2KBBestGene: %s' % current2KBBestGene)
            only2Kb[tMarkerKey] = current2KBGenes
            geneKeyToUse = current2KBBestGene
        
            
        objKey1 = tMarkerKey 
        objKey2 = geneKeyToUse
        # MGI_Relationship
        fpRelationship.write('%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s\n' % \
            (nextRelationshipKey, catKey, objKey1, objKey2, relKey, qualKey, evidKey, refsKey, userKey, userKey, cdate, cdate))

        nextRelationshipKey += 1
    
    return

# end findRelationships() -------------------------------------

# Purpose: deletes existing relationships
# Returns: None
# Assumes: None
# Effects: None
#
def writeReports():

    for tssKey in within2KbTie:
        fpWithin2KbTie.write('%s within: %s%s' % (tssKey, within2KbTie[tssKey][0], CRT))
        fpWithin2KbTie.write('%s 2KB: %s%s' % (tssKey, within2KbTie[tssKey][1], CRT))
    for tssKey in within2KbwithinClosest:
        fpWithin2KbWithinClosest.write('%s within: %s%s' % (tssKey, within2KbwithinClosest[tssKey][0], CRT))
        fpWithin2KbWithinClosest.write('%s  2KB: %s%s' % (tssKey, within2KbwithinClosest[tssKey][1], CRT))
    for tssKey in within2Kb2KbClosest:
        fpWithin2Kb2KbClosest.write('%s within: %s%s' % (tssKey, within2Kb2KbClosest[tssKey][0], CRT))
        fpWithin2Kb2KbClosest.write('%s 2KB: %s%s' % (tssKey, within2Kb2KbClosest[tssKey][1], CRT))
    for tssKey in onlyWithin:
        fpOnlyWithin.write('%s %s%s' % (tssKey, onlyWithin[tssKey], CRT))
    for tssKey in only2Kb:
        fpOnly2Kb.write('%s %s%s' % (tssKey, only2Kb[tssKey], CRT))
    for tssKey in noGene:
        fpNoGene.write('%s %s%s' % (tssKey, noGene[tssKey], CRT))

    return

# Purpose: deletes existing relationships
# Returns: None
# Assumes: None
# Effects: None
#
def doDeletes():
    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()
    db.useOneConnection(0)
    return

# end doDeletes() -------------------------------------

# Purpose: loads bcp file
# Returns: exist 2 if bcp fails
# Assumes: None
# Effects: None
#
def bcpFiles():

    bcpCommand = os.environ['PG_DBUTILS'] + '/bin/bcpin.csh'

    bcpCmd = '%s %s %s %s %s %s "|" "\\n" mgd' % \
            (bcpCommand, db.get_sqlServer(), db.get_sqlDatabase(), 'MGI_Relationship', outputDir, bcpFile)

    rc = os.system(bcpCmd)

    if rc != 0:
        closeFiles()
        sys.exit(2)

    return

# end bcpFiles() -------------------------------------

#
# Main
#

print('%s' % mgi_utils.date())
print ('init()')
# exit(1) if errors opening files
init()

print('%s' % mgi_utils.date())
print('findRelationships()')
# determine overlaps and create relationship bcp file
findRelationships()

print('%s' % mgi_utils.date())
print('writeReports()')
# write out info for each bucket of relationships
writeReports()

print('%s' % mgi_utils.date())
print('doDeletes()')
# delete existing relationships
doDeletes()

print('%s' % mgi_utils.date())
print('closeFiles()')
# close all output files
closeFiles()

print('%s' % mgi_utils.date())
print('bcpFiles()')
# exit(2) if bcp command fails
bcpFiles()

print('%s' % mgi_utils.date())
print('done')
sys.exit(0)
