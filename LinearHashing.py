from HashIndexing import *

# TODO: for each bucket page, the overflow page is just the overflow page object

class LinearHashingIndexer(Indexer):

    def __init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize):
        """
        the follwing should be stored in the header page
        - level
        - N_level
        - next

        the following should be stored in the data pages
        - overflow pointer
        """
        Indexer.__init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize)
        
        # NOTE: self.numBuckets is number of buckets initialily
        self.next = 1 # next = 1 since the bucket page starts at index 1 in memPages
        # indicates the current round number
        self.level = 0
        # number of bits to represents number of buckets at the beginning of the level <==> N_level
        self.di = math.log(self.numBuckets ,2)
        # set the *initial* number of buckets at the *beginning* of each round
        self.N_level = self.numBuckets
        # use this for the hash function, so that self.level determines the range
        self.initialBuckets = self.numBuckets
        # NOTE: this one is for in-memory usage only
        # all the overflow pages including the delelted ones are here
        self.overflowPages = []

        # initialize the header page and the initial bucket pages
        self.memPages[0] = GenericHeaderPage(self.pSize)
        for i in range(1, self.numBuckets+1):
            self.memPages[i] = LinearHashingBucketPage(self.pSize, i)
    
    def printStatistics(self):
        """Override the method in parent class"""
        finalNumBuckets = len(self.memPages[1:])
        regularIndexPages = finalNumBuckets
        indexPagesPerBucket = []
        for bucket in self.memPages[1:]:
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

    def writeToDisk(self):
        """
        write the mempages and overflow pages into memory
        """
        # TODO: write the header page information: level, next pointer, N_level
        headerPage = self.memPages[0]
        headerPage.write(int_to_byte(self.pSize, 2), 0) # NOTE: assume page size within 2 bytes since size = 1024
        headerPage.write(int_to_byte(self.indexType, 1), headerPage.tell()) # index type
        # NOTE: the following is required for hashing
        headerPage.write(int_to_byte(self.initialBuckets, 2), headerPage.tell()) # number of primary buckets at the beginning
        headerPage.write(int_to_byte(self.level, 2), headerPage.tell()) # self.level, reserved for 2 bytes
        headerPage.write(int_to_byte(self.next, 2), headerPage.tell()) # self.next, reserved for 2 bytes
        # NOTE: sizing information
        headerPage.write(int_to_byte(OVERFLOW_BUCKET_POINTER_SIZE, 1), headerPage.tell())# overflow pointer size of each page, 1 byte, since it is size of decimal 2
        headerPage.write(int_to_byte(DATAENTRYSIZE, 1), headerPage.tell()) # record entry size: 15 bytes, only 1 byte for this number
        headerPage.write(int_to_byte(RECORD_METADATA, 1), headerPage.tell()) # number of bytes occupied by the record_metadata, decimal = 2 ==> number of byte to hold it is 1
        
        # NOTE: readjust the overflow page location in the index file
        overFlowStartPos = len(self.memPages)
        i = 0
        for extendableBucketPage in self.memPages[1:]:
            # update overflow offset one by one, not recursive
            overflowPage = extendableBucketPage.getOverflowPage()
            while overflowPage:
                overflowPage.setRelativePageNum(i + overFlowStartPos)
                overflowPage = overflowPage.getOverflowPage() 
                i += 1

        # NOTE: write metadata and data entries into each page (primary or overflow)
        for extendableBucketPage in self.memPages[1:]:
            extendableBucketPage.writeNumRecordPerPage(offset=0) # write the records metadata per page
            extendableBucketPage.writeEntriesToPage(offset=RECORD_METADATA) # write the data entries start after the 2nd bytes
            extendableBucketPage.writeOverflowOffset(0)

        # write each chain into the file
        with open("./results/IndexFiles/{}".format(self.indexFileName), 'wb+') as indexPointer:

            # NOTE: write the header page separately
            indexPointer.write(headerPage.rawPage())

            # write all the primary pages for each bucket
            for page in self.memPages[1:]:
                indexPointer.write(page.rawPage())

            # TODO: does not handle the case where some overflow pages are deleted after redistribution
            # write the overflow pages after all the primary pages if necessary
            for page in self.memPages[1:]:
                # NOTE: write *each chain* at a time, exclude the primary page
                overflowPage = page.getOverflowPage()
                while overflowPage:
                    indexPointer.write(overflowPage.rawPage()) # TODO: check this
                    overflowPage = overflowPage.getOverflowPage() 
        return
        
    def hashFunc(self, i, key):
        """
        -wrapper function for the md5 function
        -double the range of hash mapping by the i

        @param i int: number of bits to represents the number of buckets inside the index
            - 'i' should be the level attribute
        """
        range_ = (2**i) * self.initialBuckets
        bucketNum = (super().hash_(key.replace('\x00', '')) % range_) + 1 # NOTE: exclude zero, the header page
        return bucketNum
    
    def _updateBookkeepings(self):
        """update necessary bookkeeping information"""
        self.level += 1
        self.next = 1
        self.N_level *= 2
    
    def  _redistributeContent(self, splitBucketIndex):
        """redistribute the contents between the bucket at self.next and splitBucketIndex
        - the content to be redistributed includes the newRecordEntry
        """
        # NOTE: case 1: bucket at self.next index has multiple overflow pages
        def _getAllEntries(bucketIndex):
            bucketPage = self.memPages[bucketIndex]
            entries = bucketPage.allEntries()
            while bucketPage.getOverflowPage() != None:
                bucketPage = bucketPage.getOverflowPage()
                entries.extend(bucketPage.allEntries())
            return entries

        currentBucketEntries = set(_getAllEntries(self.next))
        splitedBucketEntries = set([])        

        # NOTE: for each entry, hash the key, look at the last global depth of bits, and redistribute
        for recordEntry in currentBucketEntries:
            # use H_{level+1} hash function
            desiredBucket = self.hashFunc(self.level + 1, recordEntry.getKey())
            assert(self.next == desiredBucket or desiredBucket == splitBucketIndex)
            # use the property of linear hashing
            if desiredBucket == splitBucketIndex:
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

                overflowPage = bucketPage.getOverflowPage()
                # NOTE: case where the distribution of data entry is very skewed
                if overflowPage is None:
                    overflowPage = LinearHashingBucketPage(self.pSize, -9999)
                    self.overflowPages.append(overflowPage)
                    # link the overflow pages
                    bucketPage.setOverflowPage(overflowPage)
                bucketPage = overflowPage

        # NOTE: reallocate all the entries between the two buckets
        _insertAllEntries(self.next, list(currentBucketEntries))
        _insertAllEntries(splitBucketIndex, list(splitedBucketEntries))

    def _triggerSplit(self):
        """
        -split the contents of the bucket pointed by self.next
        -always split a bucket inside the self.memPages

        @return: the bucketPage , either the split page or the orignal page
        """
        # NOTE: the expantion of the primay bucket pages is here
        imageBucketIndex = self.expandIndexfile(LinearHashingBucketPage(self.pSize, len(self.memPages)))
        
        # redistribute the contents between the two splited bucket pages
        self._redistributeContent(imageBucketIndex)

        # NOTE: update the bookkeepings for the next round 
        self.next += 1
        if self.next == (self.N_level + 1): # think of N_level = 2 and self.next = 1 at the beginning of the level
            self._updateBookkeepings()

    def _appendRecord(self, bucketIndex, recordEntry):
        """
        -this insert the record into the bucketIndex
        -create a overflow page if necessary
        """
        '''find the the page linked by the bucketPage to insert or trigger a split and insert'''
        bucketPage = self.memPages[bucketIndex] 
        # NOTE: the primary page to insert the record, may be inserted into overflow pages
        while bucketPage.getSpaceRemaining() <= 0:
            #overFlowPageIndex = bucketPage.getOverflowPage()
            overflowPage = bucketPage.getOverflowPage()
        
            # NOTE: Need to create a new overflow page ==> insert the new record entry ==> trigger split
            if overflowPage is None: 
                overflowPage = LinearHashingBucketPage(self.pSize, -9999)
                self.overflowPages.append(overflowPage)
                bucketPage.setOverflowPage(overflowPage)
                overflowPage.insertEntry(recordEntry)
                self._triggerSplit() # NOTE: trigger a split from self.next
                return
            bucketPage = overflowPage

        '''bucketPage with space remain for the new record without trigger a split'''
        bucketPage.insertEntry(recordEntry)
        return

    def hash_(self, key, rowid):
        """implementation of linear hashing"""
        
        desiredBucket = self.hashFunc(self.level, key)
        # NOTE: the desired bucket has splited
        if desiredBucket < self.next:
            desiredBucket = self.hashFunc(self.level + 1, key)

        # NOTE: split operation is triggered by the creation of overflow page
        newEntry = RecordEntry(key, rowid)
        self._appendRecord(desiredBucket, newEntry)

class LinearHashingBucketPage(ExtendablePage):
    """
    -bucket page that is being pointed by a directory entry
    -this holds recordEntry objects
    """
    def __init__(self, size, pageNum, entrySize=15):
        super().__init__(size, pageNum)

