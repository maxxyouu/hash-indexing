from bitstring import BitStream
import math
import hashlib # https://docs.python.org/2/library/hashlib.html#module-hashlib
from construct import VarInt
from Constants import *
from utils import cleanStartUp, int_to_byte, byte_to_int
class Page:
    """
    create a page of size self.pagesize in memory
    """
    def __init__(self, size, capacityFunction, entrySize=15):
        """
        @param capacityFunction: function to find the capacity of the page
        """
        self.pSize = size
        self.memPage = BitStream(self.pSize  * BYTESIZE)

        # store a list of entries for this page
        self.entries = []
        # size of (key, rowid), default to 15 bytes
        self.entrySize = entrySize
        self.entriesCapacity = capacityFunction(self.pSize, self.entrySize)
        self.spaceRemaining = self.entriesCapacity

    def write(self, byteContents, bytePos):
        """write the bytecontents to atPos in self.memPage
        -bytePos is relative to the beginning of this page
        -internally, it will also advance the pointer upto the bytePos * 8 + byteLength(byteContents)
        """
        # NOTE: overwrite the contents at bytePos of this memory page with byteContents
        self.memPage.overwrite(byteContents, bytePos * BYTESIZE)

    def writeToFile(self, filePointer):
        """write the page to file"""
        self.memPage.tofile(filePointer)

    def read(self, bytePos, formatString):
        """read the content from bytePos and convert to formatString"""
        self.memPage.bytepos = bytePos
        return self.memPage.read(formatString)

    def tell(self):
        """return the byte position of the page currently in"""
        return self.memPage.bytepos

    def seek(self, absBytePos, relative=0):
        """seek to the certain byte position"""
        if relative:
            self.memPage.bytepos += absBytePos
        else:
            self.memPage.bytepos = absBytePos

    def rawPage(self):
        """return only the contents of a page"""
        return self.memPage.bytes

    def writeEntriesToPage(self, offset=0):
        """write all the raw entries in self.entries to the raw page in this object inheriate from Page"""
        self.seek(offset)
        # note, if the entry list is empty ==> automatically handles
        for entry in self.entries:
            writableEntry = entry.parseToBytes()
            self.write(writableEntry, self.tell())

    def insertEntry(self, entry):
        """
        @param dirEntry: a directory entry object
        """
        # constraint on the capacity
        if self.spaceRemaining <= 0:
            return 0

        self.spaceRemaining -= 1
        self.entries.append(entry)
        return 1

    def replaceEntries(self, entries):
        """overwrite the list of existing entries with entries
        
        - if the original entry list is bigger ==> now is shrinked
        - if the original entry list is smaller ==> now gets bigger
            assume that the size of the entries list is smaller than the entriesCapacity
        """
        size = len(entries)

        assert(size <= self.entriesCapacity)
        self.spaceRemaining = self.entriesCapacity - size

        assert(isinstance(entries, list))
        self.entries = entries
    
    def getSpaceRemaining(self):
        return self.spaceRemaining

    def getEntriesCapacity(self):
        return self.entriesCapacity

    def getEntrySize(self):
        return self.entrySize

    def allEntries(self):
        return self.entries

    def hasEntries(self):
        return self.getSpaceRemaining() < self.getEntriesCapacity()
    
    def numRecords(self):
        return len(self.entries)

class BucketPage(Page):
    """
    -bucket page that is being pointed by a directory entry
    -this holds recordEntry objects
    """
    def __init__(self, size, pageNum, capacityFunction, entrySize=15):
        # NOTE: assume the depth maximum of 255/256
        super().__init__(size, capacityFunction, entrySize)

class ExtendablePage(Page):
    """bucket page with overflow pointer"""
    def __init__(self, size, pageNum, capacityFunction=None, entrySize=15):
        
        # NOTE: 2 bytes ==> 16 bits are reserved for the overflow page number
        # NOTE: numRecords information got by 1024 -
        def _capacityFunction(pSize, entrySize):
            return math.floor(((pSize - OVERFLOW_BUCKET_POINTER_SIZE - RECORD_METADATA) / entrySize))

        # decides use the custome or the default capacity function
        func = _capacityFunction
        if capacityFunction is not None:
            func = capacityFunction
        
        super().__init__(size, func, entrySize)
    
        # TODO: make sure what is this?
        self.relativePageNum = pageNum
        # NOTE: the pointer must be a page object
        self.overflowPage = None
        # the offset is the byte position that it is located, only reserve 2 bytes for the overflow pointers
        self.overflowPointerOffset = self.pSize - OVERFLOW_BUCKET_POINTER_SIZE

        # modify the starting location of the records
        super().seek(RECORD_METADATA, 0)

    def getRelativePageNum(self):
        return self.relativePageNum

    def setRelativePageNum(self, newOffset):
        self.relativePageNum = newOffset

    def setOverflowPage(self, bucketPge):
        self.overflowPage = bucketPge

    def getOverflowPage(self):
        return self.overflowPage
    
    def getOverflowPointerOffset(self):
        return self.overflowPointerOffset
    
    def hasOverflowPage(self):
        return self.overflowPage is not None
    
    def writeOverflowOffset(self, relativePos=0, recursive=True):
        """
        -write the overflow offset to each page within this chain
        -perform the writing *recursively*
        @param relativePos: a page position for this overflow to be inserted in
        """
        # get the overflow page object of *this* page
        overflowPage = self.getOverflowPage()
        if overflowPage is None:
            return
        # write the overflow page number in this page
        super().write(int_to_byte(relativePos + overflowPage.getRelativePageNum(), OVERFLOW_BUCKET_POINTER_SIZE),
                      self.getOverflowPointerOffset())
        if not recursive:
            return
        # recursively write offset 
        overflowPage.writeOverflowOffset(relativePos)

    def writeOverflowOffsetIterative(self, relativePos=0, iterative=True):
        """
        -write the overflow offset to each page within this chain
        -perform the writing *recursively*
        @param relativePos: a page position for this overflow to be inserted in
        """
        bucket = self
        while True:
            
            overflowPage = bucket.getOverflowPage()
            if overflowPage is None:
                return
            
            # write the overflow page number in this page
            bucket.write(int_to_byte(relativePos + overflowPage.getRelativePageNum(), OVERFLOW_BUCKET_POINTER_SIZE), 
                                     bucket.getOverflowPointerOffset())
            
            if not iterative:
                return

            bucket = overflowPage

    def writeEntriesToPage(self, recursive=True, offset=0):
        """
        OVERRIDE the parent's method
        *recursively* write entry to its page objects including the overflow pages
        
        @param offset: the byte location to start write the entries
        """
        # write entries of this bucket page into the in-memory page
        super().writeEntriesToPage(offset)

        if not recursive:
            return

        # write entries located in the overflow pages
        overFlowPage = self.getOverflowPage()
        # NOTE: should automatically interatively write entries
        if overFlowPage:
            overFlowPage.writeEntriesToPage(offset=offset)

    def writeEntriesToPageIterative(self, iterative=True, offset=0):
        """
        OVERRIDE
        perform the same functionality as the one in parent class
        """
        allEntries = self.allEntries()
        bucket = self
        while True:

            bucket.seek(offset)
            # note, if the entry list is empty ==> automatically handles
            for entry in allEntries:
                writableEntry = entry.parseToBytes()
                bucket.write(writableEntry, bucket.tell())
            
            if not iterative:
                return

            overFlowPage = bucket.getOverflowPage()
            if overFlowPage is None:
                return
            allEntries = overFlowPage.allEntries()
            bucket = overFlowPage

    def writeNumRecordPerPage(self, offset=0, recursive=True):
        """assume each extendable page has number of records being recorded at the 
        beginning of each page
        """
        super().write(int_to_byte(self.numRecords(), RECORD_METADATA), offset)

        if not recursive:
            return

        # *recursively* write numRecords at the beginning of each overflow page if exists
        overflowPage = self.getOverflowPage()
        if overflowPage:
            overflowPage.writeNumRecordPerPage(offset=offset)

    def writeNumRecordPerPageIterative(self, offset=0, iterative=True):
        """assume each extendable page has number of records being recorded at the 
        beginning of each page
        """
        bucket = self
        while True:
            bucket.write(int_to_byte(bucket.numRecords(), RECORD_METADATA), offset)

            if not iterative:
                return

            overFlowPage = bucket.getOverflowPage()
            if overFlowPage is None:
                return
            bucket = overFlowPage

class Indexer:
    """base class for all the indexer"""

    def __init__(self, indexFileName, numBuckets, pSize, indexField, indexType, entrySize):

        self.indexFileName = indexFileName
        self.numBuckets = numBuckets
        self.pSize = pSize
        self.indexField = indexField
        self.indexType = indexType
        self.entrySize = entrySize
        # add one since the first page is a directory page
        self.memPages = [None] * (self.numBuckets + 1)

        # for debug purpose, assume the number of buckets intially should be power of 2
        if self.numBuckets % 2 != 0:
            print("invalid bucket number")
        
        self.statsTracker = None

    ########################################## Common Functions  ####################################################)

    def indexSizeInPage(self):
        """get number of pages occupied by the index currently"""
        return len(self.memPages)

    def expandIndexfile(self, pageObject=None):
        """return the newly created page object index at the end of the list
        """
        self.memPages.append(pageObject)
        self.numBuckets += 1
        return len(self.memPages) - 1

    def getPageAtByteOffset(self, pageByteOffset):
        """return the right page with pageByteOffset
        at the same time set the page pointer location to the correct location
        """
        ithPage = math.ceil(pageByteOffset / self.pSize)
        targetPage = self.getithPage(ithPage)
        targetPage.bytepos = pageByteOffset % self.pSize
        return targetPage

    def getithPage(self, ith):
        """return the ithpage in memory"""
        return self.memPages[ith]

    def indexFileSize(self):
        """get the index file size in bytes unit of self.psize"""
        return len(self.memPages) * self.pSize

    def write(self, byteContents, absOffset):
        """write the byteContents within the size of Index file"""
        if absOffset >= self.indexFileSize():
            print("the offset is greater than the current index file size")

        targetPage = self.getPageAtByteOffset(absOffset)
        targetPage.write(byteContents, targetPage.bytepos)

    def read(self, byteSize, absOffset):
        """
        read the byteSize of content within the index file at absoffset
        """
        if absOffset >= self.indexFileSize():
            print("the offset is greater than the current index file size")

        targetPage = self.getPageAtByteOffset(absOffset)
        return targetPage.read(byteSize)

    ############################ Functions that need to be implemented/overrided/extends by subclasses #################################

    def hash_(self, key):
        """return a integer hashed value"""
        # get the hashed value from the key
        hashFunc = hashlib.md5()
        hashFunc.update(key.encode('utf-8'))
        hashedBytes = hashFunc.digest()
        # conver the byte into integer
        return byte_to_int(hashedBytes)

    def writeToDisk(self):
        """should be called after fill the mempages"""
        # NOTE: write each page inside the mempage list to the disk
        with open("./results/IndexFiles/{}".format(self.indexFileName), 'wb+') as indexPointer:
            for page in self.memPages:
                indexPointer.write(page.rawPage())

    def printStatistics(self):
        """should print or output the results to file"""
        assert(self.statsTracker is not None)
        self.statsTracker.getStatistics()

class RecordEntry:
    """entry object abstraction for the bucket pages"""
    def __init__(self, key, rowid, fixedSize=15):
        """
        @param key: string
        @param rowid: int
        @param fixedSize: int = 15, since we use Last name (12 bytes) and rowid (3 bytes)
        """
        self.key, self.rowid = key, rowid

        # set by default
        self.keySize = FIRSTNAMESIZE
        self.rowidSize = 3

        self.fixedSize = fixedSize

    def getKey(self):
        return self.key

    def getRowid(self):
        return self.rowid

    def getEntry(self):
        return (self.key, self.rowid)

    def parseToBytes(self, valueSize=3):
        """since rowid is maximum 3 bytes"""
        # no rstrip to make sure the entry is fixed size
        writableEntry = bytes(self.key.encode()) + bytes((self.rowid).to_bytes(valueSize, "big"))
        # make sure the size is fixed
        assert(len(writableEntry) == self.fixedSize)

        return writableEntry

    def __eq__(self, other):
        return self.key == other.getKey() and self.rowid == other.getRowid()

    def __repr__(self):
        """representation of this object"""
        return "(key: {} -> rowid: {})".format(self.key, self.rowid)
    
    def __hash__(self):
        """implement this in order for this object hashable"""
        return hash((self.key, self.rowid))

class GenericHeaderPage:
    """a generic header page for the three type of indices"""
    def __init__(self, pageSize):
        self.pSize = pageSize
        self.memPage = BitStream(self.pSize  * BYTESIZE)

    def write(self, byteContents, bytePos):
        """write byte contents into the mempage at offset bytePos"""
        self.memPage.overwrite(byteContents, bytePos * BYTESIZE)

    def tell(self):
        return self.memPage.bytepos

    def rawPage(self):
        """return only the contents of a page"""
        return self.memPage.bytes

class StatsTracker:
    """statistics tracker for the index files
    
    keep tracks of the followings:
    - final number of buckets
    - number of regular index pages
    - number of overflow pages
    - number of index pages per bucket
    """
    def __init__(self, buckets, indexPages, overflowPages, pagesPerBucket, excelFileName, workSheetName):
        """assume the results passed into the parameters are final and with out changes further"""

        self.totalBuckets = buckets
        self.regularIndexPages = indexPages
        self.overflowPages = overflowPages
        # the index of the list is the ith bucket
        self.indexPagesPerBucket = pagesPerBucket

        # parameter to create a excel file
        self.excelFileName = excelFileName
        self.workSheetName = workSheetName
        self.indexName = excelFileName

    def getTotalBuckets(self):
        return self.totalBuckets
    
    def getRegularIndexPages(self):
        return self.regularIndexPages
    
    def getOverflowPages(self):
        return self.overflowPages
    
    def getIndexPagesPerBucket(self):
        return self.indexPagesPerBucket
    
    def getStatistics(self):
        print(self.indexName)
        print("Total buckets: {}".format(self.totalBuckets))
        print("Regular Index pages: {}".format(self.regularIndexPages))
        print("Overflow pages: {}".format(self.overflowPages))
        print("") # for formating purpose
        # output the index pages per bucket to display the histogram in excel
        self._writeIndexPerBucketToExcel()

    def _writeIndexPerBucketToExcel(self):
        """write the bucket index and the corresponding number to excel"""
        import xlsxwriter
        dataBook = xlsxwriter.Workbook("{}.xlsx".format(self.excelFileName))
        # parse data into csv file so that able to read from exel
        headerProperties = dataBook.add_format({'bold':True, 'align':'center', 'valign':'vcenter'})
        dataCellProperties = dataBook.add_format({'align':'center', 'valign': 'vcenter'})

        # add worksheet to the excel notebook
        pageSheet = dataBook.add_worksheet(self.workSheetName)
        pageSheet.write('A1', 'ithBucket', headerProperties)
        pageSheet.write('B1', 'indexPages', headerProperties)

        for row, indices in enumerate(self.indexPagesPerBucket):
            pageSheet.write(row+1, 0, row+1, dataCellProperties) # the bucket number, start from 1
            pageSheet.write(row+1, 1, indices, dataCellProperties) # number of index pages for this bucket
        
        dataBook.close()
    
    def plotHistogram(self, imageName):
        """plot a histogram of the index pages per bucket"""
        import matplotlib.pyplot as plt
        plt.hist(self.indexPagesPerBucket, bins=10)
        plt.ylabel('Frequency')
        plt.xlabel('Number of Index pages')
        plt.title("Index Pages for each Bucket")
        plt.savefig("./{}.png".format(imageName))
