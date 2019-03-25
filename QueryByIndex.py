from bitstring import BitStream
import math
from Constants import *
import os

import hashlib
from utils import byte_to_int

queryCondition = "Nona"
firstNameSize = 12
rowidSize = 3

class Querier:
    """abstract class for different hash index file query"""

    def __init__(self, dbFile, indexFile):
        """
        @param dbFile: database file pointer
        @param indexFile: indexFile pointer
        """
        self.dbFile = dbfile
        self.indexFile = indexFile
        self.pSize = 0
        
        self.indexPageReads = 1 # the header page is read when the object is created
        self.indexPageWritten = 0
        self.dataPageReads = 0
        self.dataPageWritten = 0

        # sizing information, overwrite the following fields by the subclasses
        self.recordCapacityoOccupiedBytes = -1
        self.recordEntrySize = -1
        self.overflowPointerSize = -1

    def incremntDataPageReads(self):
        self.dataPageReads += 1
    
    def incremntDataPageWrite(self):
        self.dataPageWritten += 1

    def incremntIndexPageReads(self):
        self.indexPageReads += 1
    
    def incremntIndexPageWrite(self):
        self.indexPageWritten += 1

    def getIndexPageReads(self):
        return self.indexPageReads
    
    def getIndexPageWrites(self):
        return self.indexPageWritten

    def getDataPageReads(self):
        return self.dataPageReads
    
    def getDataPageWrites(self):
        return self.dataPageWritten

    def hash_(self, key):
        """a generic hash function for all three hash index file"""
        hashFunc = hashlib.md5()
        hashFunc.update(key.encode('utf-8'))
        hashedBytes = hashFunc.digest()
        return byte_to_int(hashedBytes)

    def getActualRecord(self, rowids):
        """get the actual records form self.dbFile using the rowid"""
        assert(isinstance(rowids, list))
        for rowid in rowids:

            # read page unit at a time
            self.incremntDataPageReads()

            self.dbFile.seek(rowid * RECORDSIZE, 0)
            firstName = self.dbFile.read(FIRSTNAMESIZE).decode("utf-8").replace('\x00', '')
            lastName = self.dbFile.read(LASTNAMESIZE).decode("utf-8").replace('\x00', '')
            email = self.dbFile.read(EMAILSIZE).decode("utf-8").replace('\x00', '')
            print("First name: {} - Last name: {} - Email: {}".format(firstName, lastName, email))
        
        print("Number of Index page reads: {} - Number of Index page writes: {}".format(self.getIndexPageReads(), self.getIndexPageWrites()))
        print("Number of Data page reads: {} - Number of Data page writes: {}".format(self.getDataPageReads(), self.getDataPageWrites()))

    def read(self, file_, numBytes,  offset, convertToInt=1):
        """a custom read function for file at offset with numBytes, convert to the desired type"""
        file_.seek(offset)
        value = file_.read(numBytes)
        if convertToInt:
            return byte_to_int(value)
        return value.decode("utf-8")

    def queryAtBucket(self, key, desireBucket):
        """
        query all the record entries at the desired Bucket page
        """
        assert(self.pSize > 0)
        assert(self.recordCapacityoOccupiedBytes >= 0)
        assert(self.recordEntrySize >= 0)
        assert(self.overflowPointerSize >= 0)

        rowids = []
        while True:
            
            # go to the desired bucket location
            pageStartLocation = desireBucket * self.pSize
            self.indexFile.seek(pageStartLocation, 0)
            # read the number of records in this page (primary or overflow)
            numRecords = self.read(self.indexFile, self.recordCapacityoOccupiedBytes, self.indexFile.tell())
            if numRecords == 0:
                break

            # read all the byte entries from this page
            while numRecords > 0:
                
                firstName = self.read(self.indexFile, firstNameSize, self.indexFile.tell(), 0).replace('\x00', '')
                rowid = self.read(self.indexFile, rowidSize, self.indexFile.tell(), 1)
                # find a match
                if firstName == key:
                    rowids.append(rowid)
                numRecords -= 1

            # seek to the page offset location
            self.indexFile.seek(pageStartLocation + self.pSize - self.overflowPointerSize, 0)
            overflowPageid = self.read(self.indexFile, self.overflowPointerSize, self.indexFile.tell())
            # no more overflow pages
            if overflowPageid == 0:
                break
            # traverse the oveflow page for the next round of iteration
            desireBucket = overflowPageid
            self.incremntIndexPageReads()
        
        return rowids

class StaticHashIndexQuerier(Querier):

    def __init__(self, dbFile, indexFile):
        super().__init__(dbfile, indexFile)
        indexFile.seek(0)
        # NOTE: order of reading the metadata matters
        self.pSize = byte_to_int(indexFile.read(2)) # read 2 bytes from the beginning of the file
        self.indexType = byte_to_int(indexFile.read(1))
        self.initialBucketNum = byte_to_int(indexFile.read(2)) # read 2 bytes from offset of 3
        self.overflowPointerSize =  byte_to_int(indexFile.read(1))
        self.recordEntrySize = byte_to_int(indexFile.read(1))
        self.recordCapacityoOccupiedBytes = byte_to_int(indexFile.read(1))

    def hashFunc(self, key):
        # NOTE: add one since the first page is the header page, not bucket
        return (super().hash_(key.replace('\x00', '')) % self.initialBucketNum) + 1

    def queryBy(self, key): # TODO: change back to Nona when produce results
        desireBucket = self.hashFunc(key)
        assert(desireBucket >= 1 and desireBucket <= self.initialBucketNum)

        rowids = self.queryAtBucket(key, desireBucket)
        self.getActualRecord(rowids)

class LinearHashIndexQuerier(Querier):
    
    def __init__(self, dbFile, indexFile):
        super().__init__(dbfile, indexFile)
        
        indexFile.seek(0)
        self.pSize = byte_to_int(indexFile.read(2)) # read 2 bytes from the beginning of the file
        
        indexFile.seek(3, 0)
        self.initialBuckets = byte_to_int(indexFile.read(2)) # read 2 bytes from offset of 3
        
        indexFile.seek(5, 0)
        self.level = byte_to_int(indexFile.read(2))

        indexFile.seek(7, 0)
        self.next = byte_to_int(indexFile.read(2))

        self.overflowPointerSize =  byte_to_int(indexFile.read(1))
        self.recordEntrySize = byte_to_int(indexFile.read(1))
        self.recordCapacityoOccupiedBytes = byte_to_int(indexFile.read(1))

    def hashUtil(self, i, key):
        """
        NOTE: same hash function used in the linear hashing index
        """
        range_ = (2**i) * self.initialBuckets
        bucketNum = (super().hash_(key.replace('\x00', '')) % range_) + 1 # NOTE: exclude zero, the header page
        return bucketNum
    
    def hashFunc(self, key):

        desiredBucket = self.hashUtil(self.level, key)
        # NOTE: the desired bucket has splited
        if desiredBucket < self.next:
            desiredBucket = self.hashUtil(self.level + 1, key)
        return desiredBucket

    def queryBy(self, key):
        desireBucket = self.hashFunc(key)
        rowids = self.queryAtBucket(key, desireBucket)  
        self.getActualRecord(rowids)

class ExtendibleHashIndexQuerier(Querier):
    
    def __init__(self, dbFile, indexFile):
        super().__init__(dbfile, indexFile)
        indexFile.seek(0)
        # NOTE: the order of reads matters
        self.pSize = byte_to_int(indexFile.read(2))
        self.indexType = byte_to_int(indexFile.read(1))
        self.directoryStartPage = byte_to_int(indexFile.read(1))
        self.globalDepth = byte_to_int(indexFile.read(1))
        self.overflowPointerSize = byte_to_int(indexFile.read(1))
        self.directoryEntrySize = byte_to_int(indexFile.read(1))
        self.recordEntrySize = byte_to_int(indexFile.read(1))
        self.depthOccupiedBytes = byte_to_int(indexFile.read(1))
        # the bytes occupied by the record metadata, both directory and record page
        self.recordCapacityoOccupiedBytes = byte_to_int(indexFile.read(1))

    def hashFunc(self, key, lastn):
        """
        NOTE: a hash function from extendible hash index
        @param lastn: the lastn bits to be look for
        @return a decimal hashed value for the lastn bits, which is the ith bucket entry in self.directoryBuckets
        """
        full = super().hash_(key.replace('\x00', ''))
        hashedValue = int(format(full, 'b')[-lastn:], 2)
        return hashedValue

    def retrieveDirectory(self, desiredBucket):
        """by the algorithm, we read in the whole directory into memory first before we proceed"""
        counter = 0
        nextDirectoryPageLocation = self.directoryStartPage
        dirPages = 0
        directoryEntries = []
        while True:
            # found what i need
            if counter >= desiredBucket:
                break

            self.incremntIndexPageReads()
            dirPages += 1
            pageStartLocation = nextDirectoryPageLocation * self.pSize
            self.indexFile.seek(pageStartLocation, 0)

            # read the number of entries in this page (primary or overflow)
            numDirs = self.read(self.indexFile, self.recordCapacityoOccupiedBytes, self.indexFile.tell())
            counter += numDirs

            # the end of the directory chain
            if numDirs == 0:
                break

            # collect each directory entries from this directory page
            while numDirs > 0:
                dirEntry = self.read(self.indexFile, self.directoryEntrySize, self.indexFile.tell())
                directoryEntries.append(dirEntry)
                numDirs -= 1
            # seek to the directory page offset location
            self.indexFile.seek(pageStartLocation + self.pSize - OVERFLOW_BUCKET_POINTER_SIZE, 0)
            overflowPageid = self.read(self.indexFile, OVERFLOW_BUCKET_POINTER_SIZE, self.indexFile.tell())

            # no more overflow pages
            if overflowPageid == 0:
                break       
            # traverse the oveflow page for the next round of iteration
            nextDirectoryPageLocation = overflowPageid - 1
        print(dirPages)
        return directoryEntries

    def queryAtBucket(self, key, desireBucket):
        """
        Overwrite the method
        query all the record entries at the desired Bucket page
        """
        rowids = []
        
        ''' handle the header bucket pace is a special case'''
        # go directly to the record capacity location
        pageStartLocation = desireBucket * self.pSize
        self.indexFile.seek(desireBucket * self.pSize + self.depthOccupiedBytes, 0)
        # read the number of records in this page (primary or overflow)
        numRecords = self.read(self.indexFile, RECORD_METADATA, self.indexFile.tell())
        if numRecords == 0:
            return

        # read all the byte entries from this page
        while numRecords > 0:
            firstName = self.read(self.indexFile, firstNameSize, self.indexFile.tell(), 0).replace('\x00', '')
            rowid = self.read(self.indexFile, rowidSize, self.indexFile.tell(), 1)
            # find a match
            if firstName == key:
                rowids.append(rowid)
            numRecords -= 1

        # seek to the page offset location
        self.indexFile.seek(pageStartLocation + self.pSize - self.overflowPointerSize, 0)
        nextBucket = self.read(self.indexFile, self.overflowPointerSize, self.indexFile.tell())

        if nextBucket > 0:
            # the remaining bucket pages
            rowids.extend(super().queryAtBucket(key, nextBucket))
        return rowids

    def queryBy(self, key):

        # only need to look at the last globaldepth of bits
        desiredBucket = self.hashFunc(key, self.globalDepth)
        # get all the directory entry by following the chain of directory pages
        directoryEntries = self.retrieveDirectory(desiredBucket)
        
        desiredBucketPage = directoryEntries[desiredBucket]

        rowids = self.queryAtBucket(key, desiredBucketPage)
        self.getActualRecord(rowids)
        

if __name__ == "__main__":
    pSize = 1024
    field = 0
    indexType = 1
    heapFilePath = "./names.db"
    numBuckets = 512

    # NOTE: condition needed to break out the while loop below
    import os
    byteSize = os.path.getsize(heapFilePath)
    # NOTE: rediect all the outputs to the file
    # import sys
    # outpuFile = open('./results/part3_outputs.txt', 'w')
    # sys.stdout = outpuFile
    # ["staticIndex", "extendibleIndex", "linearHashIndex"]
    for indexFileName in ["extendibleIndex"]:
        dbfile = open(heapFilePath, 'rb')
        indexFile = open("./{}".format(indexFileName), 'rb')

        # print the found records for each index file
        if indexFileName == "staticIndex":
            print("Static Hash Index Outputs:")
            querier = StaticHashIndexQuerier(dbfile, indexFile)
        elif indexFileName == "extendibleIndex":
            print("Extendible Hash Index Outputs:")
            querier = ExtendibleHashIndexQuerier(dbfile, indexFile)
        else:
            print("Linear Hash Index Outputs:")
            querier = LinearHashIndexQuerier(dbfile, indexFile)
        querier.queryBy(key=queryCondition)
        print("") # format the file nicely
        dbfile.close()
        indexFile.close()
    
    # outpuFile.close()
    
