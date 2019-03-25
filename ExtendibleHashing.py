from HashIndexing import *
from bitstring import ConstBitStream, BitArray
from Constants import *
import math
from utils import int_to_bin

class ExtendibleHashingIndexer(Indexer):
    """extendible hashing index implementation"""

    def __init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize):

        Indexer.__init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize)

        # extra page for directory
        self.expandIndexfile()

        # number of bits to be looked for
        self.globalDepth = math.ceil(math.log(numBuckets, 2))
        # make sure it is power of 2
        assert((numBuckets % 2)== 0)

        self.initialNumBuckets = numBuckets

        # NOTE: the two special pages for the hashing index
        self.memPages[0] = GenericHeaderPage(self.pSize) # set the header page
        self.memPages[1] = DirectoryPage(self.pSize, self.globalDepth, 1) # set the first directory page

        # insert the following pages after all the buckets, except the first directory page
        self.bucketOverflowPages = [] # NOTE: may contains garbage overflow pages after redistribution of entries
        self.directoryPages = [] # NOTE: that the directory pages are linked together, only on chain
        self.directoryPages.append(self.memPages[1])

        # NOTE: store all the directory entry, when update the pageid within each entry object, operate on this
        # and should reflect on the pages since they are the same object
        self.directoryBuckets = [] 
        
        # NOTE: populate the bucket pages
        for i in range(2, numBuckets+2):
            self.memPages[i] = ExtendibleHashBucketPage(self.pSize, self.globalDepth, i, headBucket=True) # at the beginning: globaldepth = local depth

        # NOTE: all the bucket pages are offseted by 2 from the beginning of the file
        for ithBucket, pageid in enumerate(range(2, numBuckets + 2)):
            self.appendEntryToDirectory(DirectoryEntry(ithBucket, pageid)) # append the last directory page in the linked list of directory

    ################################ utility function ################################

    def updateGlobalDepth(self):
        self.globalDepth += 1

    def hashFunc(self, key, lastn):
        """
        a customed hashfunction for the extendible hash index
        @param lastn: the lastn bits to be look for
        @return a decimal hashed value for the lastn bits, which is the ith bucket entry in self.directoryBuckets
        """
        full = super().hash_(key.replace('\x00', ''))
        hashedValue = int(format(full, 'b')[-lastn:], 2)
        return hashedValue, full

    ################################ utility function ################################
    def printStatistics(self):
        """Override the method in parent class"""

        # construct the statistic object
        finalNumBuckets = len(self.memPages[2:]) # exclude the header page and the first directory page
        regularIndexPages = len(self.directoryPages) + finalNumBuckets # all directory pages and the first page of each bucket
        # construct index pages per bucket
        indexPagesPerBucket = []
        for bucket in self.memPages[2:]:
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
        overflowPages = sum(indexPagesPerBucket) - finalNumBuckets

        self.statsTracker = StatsTracker(finalNumBuckets, regularIndexPages, overflowPages, indexPagesPerBucket, self.indexFileName, self.indexFileName)
        super().printStatistics()
        return self.statsTracker

    def updateDirEntryPageid(self, ithDirBucket, newPageid):
        """change the directory entry 's page id"""
        # NOTE: -1 since the ithDirBucket start from 1, but the the self.directoryBuckets list starts from 0
        self.directoryBuckets[ithDirBucket].updateEntry(newPageid)

    def ithBucketPage(self, ithDirBucket):
        """get the ith directory entry(bucket) from the array"""
        targetDirEntry = self.directoryBuckets[ithDirBucket]
        return self.memPages[targetDirEntry.getPageId()]

    def appendEntryToDirectory(self, dirEntry):
        """return the directory page that the dirEntry is inserted"""
        assert(isinstance(dirEntry, DirectoryEntry))

        '''add a directory entry into self.directoryBuckets'''
        self.directoryBuckets.append(dirEntry)
        
        '''add the same directory entry into the directory page'''
        # NOTE: get the last directory page and it has no overflow pages for sure
        targetDirectory = self.directoryPages[-1]
        assert(targetDirectory.getOverflowPage() is None)

        if targetDirectory.getSpaceRemaining() <= 0: # create overflow directory pages
            # create new directory page
            newDirectory = DirectoryPage(self.pSize, self.globalDepth, len(self.directoryPages))
            # *link* the directory page in self.mempages
            targetDirectory.setOverflowPage(newDirectory)
            # insert the directory page into the memPage list
            self.directoryPages.append(newDirectory)
            targetDirectory = newDirectory

        targetDirectory.insertEntry(dirEntry)

    def writeToDisk(self):
        """convert all the abstraction pages to disk"""
        # print("primary buckets:{} - overflow buckets:{} - directories: {} - globalDepth: {}".format(len(self.memPages)-2, len(self.bucketOverflowPages), len(self.directoryPages), self.globalDepth))
       
        # NOTE: insert header page information
        headerPage = self.memPages[0]
        headerPage.write(int_to_byte(self.pSize, 2), 0) # NOTE: assume page size within 2 bytes
        headerPage.write(int_to_byte(self.indexType, 1), headerPage.tell()) # index type
        headerPage.write(int_to_byte(1, 1), headerPage.tell()) # directory page starts location
        headerPage.write(int_to_byte(self.globalDepth, 1), headerPage.tell()) # global depth, 1 byte
        # NOTE: sizing information
        headerPage.write(int_to_byte(OVERFLOW_BUCKET_POINTER_SIZE, 1), headerPage.tell())# overflow pointer size of each page, 1 byte, since it is size of decimal 2
        headerPage.write(int_to_byte(2, 1), headerPage.tell()) # the directory entry size : 2 bytes
        headerPage.write(int_to_byte(DATAENTRYSIZE, 1), headerPage.tell()) # record entry size: 15 bytes, only 1 byte for this number
        headerPage.write(int_to_byte(DEPTH_SIZE, 1), headerPage.tell()) # the local depth size for each head bucket page
        headerPage.write(int_to_byte(RECORD_METADATA, 1), headerPage.tell()) # number of bytes occupied by the record_metadata, decimal = 2 ==> number of byte to hold it is 1
        #headerPage.write(int_to_byte(self.memPages[0].getEntriesCapacity(), 1), headerPage.tell()) # write the directory entry capacity

        '''use the relative position for the following'''
        # insert bucket pages first, start from index 2 ==> skips the headaer page and the first directory page
        overflowPageStartOffset = len(self.memPages)
        for bucketPage in self.memPages[2:]:
            # write the depth metadata for the head bucket page at offset = 0
            bucketPage.writeDepthMetadata()
            # write the records number that each page has, EXCLUDING overflow pages
            bucketPage.writeNumRecordPerPageIterative(offset=bucketPage.depthSize, iterative=False)
            # write the entries start at offset bucketPage.depthSize+RECORD_METADATA, ONLY FOR HEADER BUCKET PAGE
            bucketPage.writeEntriesToPageIterative(offset=bucketPage.depthSize+RECORD_METADATA, iterative=False)
            # RECURSIVELY write the overflow offset into the primary bucket page and overflow pages
            bucketPage.writeOverflowOffsetIterative(relativePos=overflowPageStartOffset)

        # write entries and records contained into each page recursively NOTE: EXCLUDING the header page of each bucket chain
        for bucketPage in self.memPages[2:]:
            overflow = bucketPage.getOverflowPage()
            # write the entries start right after the record metadata
            if overflow is not None:
                overflow.writeNumRecordPerPageIterative(offset=0, iterative=True)
                overflow.writeEntriesToPageIterative(offset=RECORD_METADATA, iterative=True)

        # NOTE: write directory entries and overflow offset into each directory pages, use *iterative* method
        headDirectory = self.directoryPages[0]
        headDirectory.writeNumRecordPerPageIterative(iterative=True)
        headDirectory.writeEntriesToPageIterative(offset=RECORD_METADATA, iterative=True) # assume two bytes off the page pointer
        headDirectory.writeOverflowOffsetIterative(relativePos=len(self.memPages)+len(self.bucketOverflowPages))

        # write all the raw pages into the index file
        with open("./results/IndexFiles/{}".format(self.indexFileName), 'wb+') as indexPointer:
            # NOTE: order matters
            allPages = self.memPages + self.bucketOverflowPages + self.directoryPages[1:]
            for page in allPages:
                indexPointer.write(page.rawPage())
        return

    def _splitBucket(self, bucketIndex, newRecordEntry, fullHashedValue):
        """
        @param bucketIndex: the ith bucket in the directory entries
        @param newRecordEntry: a record object that also need to be added when distribute the contents
        beween the self.memPages[bucketIndex] and its splited image
        """
        currentBucketPage = self.ithBucketPage(bucketIndex)
        assert(currentBucketPage.getSpaceRemaining() == 0)

        oldLocalDepth = currentBucketPage.getDepth()
        newLocalDepth = oldLocalDepth + 1
        currentBucketPage.updateDepth(newLocalDepth)

        # correctness checking
        assert(len(set([self.hashFunc(entry.getKey(), oldLocalDepth)[0] for entry in currentBucketPage.allEntries()])) <= 1)

        # NOTE: allocate a splited bucket for redistributions
        imageBucketIndex = self.expandIndexfile(ExtendibleHashBucketPage(self.pSize, newLocalDepth, len(self.memPages), headBucket=True))
        
        # NOTE: get the splited bucket and the old bucket index
        splitedDirBin = '1' + format(fullHashedValue, 'b')[-oldLocalDepth:]

        # NOTE: redistribute the contents between the original bucket page and the splited(newly created) bucket page
        self._redistributeContent(self.directoryBuckets[bucketIndex].getPageId(), imageBucketIndex, newRecordEntry, newLocalDepth)

        # NOTE: when the new local depth exceed the global depth
        if oldLocalDepth == self.globalDepth:
            splitedDirIndex = int(splitedDirBin, 2)
            self._doublingDirectory(splitedDirIndex, imageBucketIndex) # this increment the global depth by 1, to discriminate thes
        else:
            # traverse the directory buckets to readjust pointers
            for index, directoryBucket in enumerate(self.directoryBuckets):
                binString = int_to_bin(index, self.globalDepth)
                if binString[-newLocalDepth:] == splitedDirBin:
                    directoryBucket.updateEntry(imageBucketIndex)

    def _redistributeContent(self, oldBucketIndex, splitBucketIndex, newRecordEntry, newDepth):
        """
        -redistribute the content between the old bucket at oldBucketIndex and new bucket at splitBucketIndex
        -note that bucket at oldBucketIndex and bucket at splitBucketIndex may contains overflow pages

        @param oldBucketIndex: the index of self.memPages
        @param splitBucketIndex: the index of self.memPages
        """
        # NOTE: case 1: bucket at self.next index has multiple overflow pages
        def _getAllEntries(bucketIndex):
            bucketPage = self.memPages[bucketIndex]
            entries = bucketPage.allEntries()
            while bucketPage.getOverflowPage() != None:
                bucketPage = bucketPage.getOverflowPage()
                entries.extend(bucketPage.allEntries())
            return entries

        currentBucketEntries = set(_getAllEntries(oldBucketIndex) + [newRecordEntry]) # NOTE: includes the new record entry to be redistributed
        splitedBucketEntries = set([])

        # NOTE: for each entry, hash the key, look at the last global depth of bits, and redistribute
        for recordEntry in currentBucketEntries:
            _, fullHash = self.hashFunc(recordEntry.getKey(), newDepth) # use H_{level+1} hash function
            discriminateBit = format(fullHash, 'b')[-newDepth]
            if discriminateBit == ONE: # NOTE: if they points to the same bucket
                splitedBucketEntries |= set([recordEntry])
        # split the entry set between the current bucket and the splited bucket
        currentBucketEntries -= splitedBucketEntries

        # NOTE: case2: splited bucket might also has multiple overflow pages
        def _insertAllEntries(bucketIndex, entries):
            bucketPage = self.memPages[bucketIndex]
            # NOTE: assume bucketPage capacity is the same across all of them
            limits, bound = bucketPage.getEntriesCapacity(), len(entries)
            # NOTE: the rolling window determine how many records to be inserted into the page
            rollingWindow = (0, min(limits, bound))
            # NOTE: keep rolling until the window is closed
            while True:
                # Override the record entries list
                bucketPage.replaceEntries(entries[rollingWindow[0]: rollingWindow[1]])
                # update rolling window for the next round if there are any
                rollingWindow = (rollingWindow[1], rollingWindow[1] + min(bound - rollingWindow[1], limits))
                # NOTE: done inserting entries and terminate the bucket chain
                if rollingWindow[0] >= rollingWindow[1]:
                    bucketPage.setOverflowPage(None)
                    return

                '''case where there entries left, one bucket page is not enough'''

                overflowPage = bucketPage.getOverflowPage()
                # NOTE: case where the distribution of data entry is very skewed
                if overflowPage is None:
                    overflowPage = ExtendibleHashBucketPage(self.pSize, newDepth, len(self.bucketOverflowPages))
                    self.bucketOverflowPages.append(overflowPage)
                    # link the overflow pages
                    bucketPage.setOverflowPage(overflowPage)
                bucketPage = overflowPage

        # NOTE: reallocate all the entries between the two buckets
        _insertAllEntries(oldBucketIndex, list(currentBucketEntries))
        _insertAllEntries(splitBucketIndex, list(splitedBucketEntries))

    def _doublingDirectory(self, splitDirectoryEntry, bucketIndex):
        """
        -this double the current directory and may need to link multiple directory
        -reorganize the pointers to the directory page (When replace the content, keep track of this)
        -increase the global depth at the same time

        @param splitDirectoryEntry: the index of the splited bucket
        @param bucketIndex: the primary bucket page index in self.memPages
        """
        totalDirEntries = len(self.directoryBuckets) # this determine how many buckets in the index file
        doubledEntriesSize = 2 * totalDirEntries

        # NOTE: point all the new directory bucket to the orinal one and check assumptions that number of buckets always power of two
        assert((totalDirEntries % 2) == 0)
        for i in range(totalDirEntries, doubledEntriesSize):
            pageId = self.directoryBuckets[i - totalDirEntries].getPageId()
            if i == splitDirectoryEntry:
                pageId = bucketIndex
            self.appendEntryToDirectory(DirectoryEntry(i, pageId))
        assert((len(self.directoryBuckets) % 2 == 0))
        assert(self.directoryBuckets[splitDirectoryEntry].getPageId() == bucketIndex)

        # NOTE: increase the global depth
        self.updateGlobalDepth()

    def hash_(self, key, rowid):
        """
        perform a hash function on extendible hash index

        @param key: string with trailing spaces
        @param rowid: int
        """
        newEntry = RecordEntry(key, rowid)
        # hash to a desired bucket
        desiredBucket, fullHashedDecimal = self.hashFunc(key, self.globalDepth)
        # use the directory entries to find the head bucket page
        bucketPage = self.ithBucketPage(desiredBucket)
        # print("desired bucket: {}".format(format(fullHashedDecimal, 'b')[-self.globalDepth:]))
        
        # NOTE: current bucket page is full, requires overflow page of this bucketPage
        while True:
            if bucketPage.getSpaceRemaining() > 0:
                break
            overflowPage = bucketPage.getOverflowPage()
            if overflowPage is None:
                # NOTE: split the bucket pointed by the directory's desiredBucket
                self._splitBucket(desiredBucket, newEntry, fullHashedDecimal)
                return
            bucketPage = overflowPage # keep iterate the chain of overflow pages

        # NOTE: the current bucket page has space to insert a new record
        bucketPage.insertEntry(newEntry)

class DirectoryEntry:
    """entry object abstraction for the directory page"""
    def __init__(self, ithDirEntry, pageId):
        """
        @param ithDirEntry: the entry number in the directory in decimal, aka: the ith bucket in directory
        @param pageId: the page number of the entry points to
        """
        # for inmemory usage only
        self.ithDirEntry = ithDirEntry
        # entry that need to be written to disk, NOTE: the page id
        self.pageId = pageId

    def getPageIdNum(self):
        return self.ithDirEntry

    def getPageId(self):
        return self.pageId

    def updateEntry(self, newPageId):
        self.pageId = newPageId

    def parseToBytes(self, fixedSize=2):
        """no need to write the pageNum, the order is the pageNum"""
        return bytes((self.pageId).to_bytes(fixedSize, "big"))

    def __eq__(self, other):
        return self.ithDirEntry == other.getPageIdNum() and self.pageId == other.getPageId()

class DirectoryPage(ExtendablePage):
    """
    -a directory page that holds the pointer to a bucket page
    -this holds directoryEntry objects
    """
    def __init__(self, size, globalDepth, pageNum, entrySize=2):
        """
        @param pointerSize: the page number, default to TWO bytes page pointer
        """
        # assume also 2 bytes for the number of directory entries inside the directory page
        ExtendablePage.__init__(self, size, pageNum, entrySize=entrySize)
        self.depth = globalDepth

    def getDepth(self):
        return self.depth

    def updateDepth(self, newDepth):
        self.depth = newDepth

class ExtendibleHashBucketPage(ExtendablePage):
    """
    -bucket page that is being pointed by a directory entry
    -this holds recordEntry objects
    - this may has overflow pages
    """
    def __init__(self, size, localDepth, pageNum, headBucket=False, entrySize=15):
        # two byte for the depth metadata, that is at the worst case, two bytes for the overflow pages
        
        def _capacityHead(psize, entrySize):
            """capacity minus depths and record size"""
            remaining = psize - OVERFLOW_BUCKET_POINTER_SIZE - DEPTH_SIZE - RECORD_METADATA
            return math.floor(remaining / entrySize) # NOTE: assume the depth maximum of 255/256
        
        def _capacityRegular(psize, entrySize):
            """no depth parameter in this page"""
            remaining = psize - OVERFLOW_BUCKET_POINTER_SIZE - RECORD_METADATA
            return math.floor(remaining / entrySize)

        if headBucket:
            super().__init__(size, pageNum, capacityFunction=_capacityHead)
        else:
            super().__init__(size, pageNum, capacityFunction=_capacityRegular)

        self.depth = localDepth
        self.headBucket = headBucket

        # only the head bucket page has reserved bytes for the depth parameters
        if headBucket:
            super().seek(DEPTH_SIZE + RECORD_METADATA, 0) # NOTE THIS
            # number of bytes reserved the depth parameter
            self.depthSize = DEPTH_SIZE
    
    def getDepthSize(self):
        return self.depthSize

    def getDepth(self):
        return self.depth

    def updateDepth(self, newDepth):
        self.depth = newDepth

    def isHeadBucketPage(self):
        return self.headBucket
    
    def writeDepthMetadata(self):
        """write the depth metadata for this page"""
        assert(self.headBucket is True)
        super().write(int_to_byte(self.depth, self.depthSize), 0)

