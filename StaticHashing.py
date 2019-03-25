from HashIndexing import *
from bitstring import ConstBitStream
from utils import int_to_byte, byte_to_int

class staticHashingIndexer(Indexer):
    """static hash index constructor"""

    def __init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize):

        Indexer.__init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize)

        self.initialBucketNum = numBuckets
        # initialized a fixed number of buckets for the index file
        self.memPages[0] = GenericHeaderPage(self.pSize)
        for i in range(1, self.numBuckets+1):
            self.memPages[i] = StaticHashingBucketPage(self.pSize, i)
    
    def hashFunc(self, key):
        return (super().hash_(key.replace('\x00', '')) % self.initialBucketNum) + 1

    def printStatistics(self):
        """Override the method in parent class"""

        # construct the statistic object
        finalNumBuckets = self.initialBucketNum
        regularIndexPages = self.initialBucketNum
        overflowPages = len(self.memPages[1:]) - self.initialBucketNum

        # construct index pages per bucket
        indexPagesPerBucket = []
        for bucket in self.memPages[1:self.initialBucketNum + 1]:
            indexPages = 1 # the bucket page itself
            temp = bucket
            while True:
                overflowPage = temp.getOverflowPage()
                if overflowPage is None:
                    break
                temp = overflowPage
                indexPages += 1
            # store the results
            indexPagesPerBucket.append(indexPages)

        assert(len(indexPagesPerBucket) == self.initialBucketNum)
        self.statsTracker = StatsTracker(finalNumBuckets, regularIndexPages, overflowPages, indexPagesPerBucket, self.indexFileName, self.indexFileName)
        super().printStatistics()
        return self.statsTracker

    def writeToDisk(self):
        """convert all the abstractions pages to disk"""
        # NOTE: write the header page
        headerPage = self.memPages[0]
        headerPage.write(int_to_byte(self.pSize, 2), 0) # NOTE: assume page size within 2 bytes since size = 1024
        headerPage.write(int_to_byte(self.indexType, 1), headerPage.tell()) # index type
        headerPage.write(int_to_byte(self.initialBucketNum, 2), headerPage.tell()) # number of buckets
        headerPage.write(int_to_byte(OVERFLOW_BUCKET_POINTER_SIZE, 1), headerPage.tell())# overflow pointer size of each page, 1 byte, since it is size of decimal 2
        headerPage.write(int_to_byte(DATAENTRYSIZE, 1), headerPage.tell()) # record entry size: 15 bytes, only 1 byte for this number
        headerPage.write(int_to_byte(RECORD_METADATA, 1), headerPage.tell()) # number of bytes occupied by the record_metadata, decimal = 2 ==> number of byte to hold it is 1

        # NOTE: ignore the overflow pages
        for page in self.memPages[1:self.initialBucketNum + 1]:
            if page.hasEntries():
                page.writeNumRecordPerPage(offset=0)
                page.writeEntriesToPage(offset=RECORD_METADATA) # NOTE: write the raw entries to the pages
                # NOTE: write the offset recursively; also the offset is absolute relative to the beginning of the self.memPages
                page.writeOverflowOffset(relativePos=0) 

        # write to the disk
        super().writeToDisk()

    def hash_(self, key, rowid):
        """perform static hashing"""

        # NOTE: + 1 to exclude the directory page at index 0 and hash the stripped key
        desiredBucket = self.hashFunc(key)
        assert(desiredBucket > 0 and desiredBucket <= len(self.memPages))   
        bucketPage = self.memPages[desiredBucket]

        # keep finding a page that has space or allocate a new page
        while bucketPage.getSpaceRemaining() <= 0:

            # get the oveflow pointer
            overflowPage = bucketPage.getOverflowPage()
            if overflowPage == None: 
                # NOTE: allocate a new overflow page for this page at the end of the index file, aka self.memPages
                overflowPage = StaticHashingBucketPage(self.pSize, len(self.memPages))
                self.expandIndexfile(overflowPage)
                bucketPage.setOverflowPage(overflowPage)

            bucketPage = overflowPage

        # NOTE: insert the new record entry into the overflow page or the primary index page as a record object
        bucketPage.insertEntry(RecordEntry(key, rowid))

    ################################# Part 3 function implementation ####################33

class StaticHashingBucketPage(ExtendablePage):
    """
    -bucket page that is being pointed by a directory entry
    -this holds recordEntry objects
    -has overflow pointer
    """
    def __init__(self, size, pageNum, entrySize=15):
        ExtendablePage.__init__(self, size, pageNum)
