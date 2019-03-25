from bitstring import BitStream
import math
from Constants import *
import os
from utils import cleanStartUp, int_to_byte, byte_to_int

class Record:
    """individual record object"""
    
    def __init__(self, recordSize, record):
        self.recordSize = recordSize
        # a triple (first name, last anme, email)
        self.record = record
    
    def parseToBytes(self):
        """convert the record triple to a sequence of bytes"""
        first, last, email = self.record[0], self.record[1], self.record[2]
        return bytes(first.encode()) + bytes(last.encode()) + bytes(email.encode())

    def getRecord(self):
        return self.record

    def getField(self, field):
        """assume only three fields in a record"""
        return self.record[field]
    
    def __lt__(self, other):
        """compare the field at index 1, fixed by the assignment"""
        return self.record[1].replace('\x00', '') < other.getRecord()[1].replace('\x00', '')
    
    def __repr__(self):
        cleanRecord = [field.strip() for field in self.record]
        return str(cleanRecord)

class RecordPage:
    """
    create a page of size self.pagesize *in memory*
    """
    def __init__(self, size, byteContents=None):
        self.pSize = size

        if not byteContents:
            self.memPage = BitStream(self.pSize * BYTESIZE)
        else:
            self.memPage = BitStream(byteContents)
            self.memPage.bytepos = len(self.memPage) // 8

    def rawRecordPage(self):
        """return a byte object
        - if the page is partially filled, it only return the filled contents
        """
        # NOTE: the indexing syntax is interms of bits
        return self.memPage[:self.memPage.pos].bytes

    def overWrite(self, byteContents, offset=0):
        """write the bytecontents to atPos in self.memPage
        -bytePos is relative to the beginning of this page
        -internally, it will also advance the pointer upto the bytePos * 8 + byteLength(byteContents)
        """
        # overwrite the contents at offset
        self.memPage.bytepos = offset
        self.memPage.overwrite(byteContents,self.memPage.pos)

        if self.memPage.bytepos >= self.pSize:
            return 1
        return 0

    def write(self, filePointer, offset):
        """write the page to file"""
        filePointer.seek(offset, 0)
        filePointer.write(self.rawRecordPage())
        # clean up the in memory page
        self.flush()

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
            self.memPage.bytepos += absBytePos * BYTESIZE
        else:
            self.memPage.bytepos = absBytePos * BYTESIZE
    
    def pageSize(self):
        return self.pSize

    def getAllRecords(self):
        """return a list of record objects in page bufferPageIndex
        assume the page is filled
        """
        pageRecordList = []
        # NOTE: the last record should ends at self.memPage.bytepos through overwrite() methods
        for bytePos in range(0, self.memPage.bytepos, RECORDSIZE):
            # starting record offset
            firstName = self.read(bytePos, "bytes:{}".format(FIRSTNAMESIZE)).decode("utf-8")
            lastName = self.read(self.tell(), "bytes:{}".format(LASTNAMESIZE)).decode("utf-8")
            email = self.read(self.tell(), "bytes:{}".format(EMAILSIZE)).decode("utf-8")
            pageRecordList.append(Record(RECORDSIZE, (firstName, lastName, email)))
            # NOTE: includes Null character after each field
        return pageRecordList
    
    def hasRecords(self):
        return self.memPage.bytepos > 0
    
    def flush(self):
        self.memPage = BitStream(self.pSize * BYTESIZE)

class Buffer:
    """each Buffer object contains bufferSize number of pages and pageSize bytes for each page """
    def __init__(self, pageSize, bufferSize, sortField, byteContents=[]):
        self.pSize = pageSize
        self.bufferSize = bufferSize

        if len(byteContents) == bufferSize:
            result = [RecordPage(self.pSize, byteContents[i]) for i in range(0, self.bufferSize)]
        else:
            result = [RecordPage(self.pSize) for _ in range(0, self.bufferSize)] 
        self.buffer = result

        self.fillIndicator = [0] * self.bufferSize
        # store a list of record object everytime calls fill, one to one correspondent to the byte contents
        self.recordsPerPage = [[]] * self.bufferSize
        self.outputBufferPage = self.buffer[-1]
        self.sortField = sortField
    
    def getBufferSize(self):
        return self.bufferSize
    
    def isFilled(self, pageIndex):
        return self.fillIndicator[pageIndex]
    
    def _checkFull(self):
        if sum(self.fillIndicator) == self.bufferSize:
            print("no more space")
            return 0
        return 1
    
    def _checkBound(self, bufferPageIndex):
        if bufferPageIndex >= self.bufferSize:
            print("buffer size is fixed")
            return 0
        return 1
        
    def _pageIsFilled(self, index):
        if not self.fillIndicator[index]:
            return 0
        return 1

    def fillBuffer(self, byteContents, bufferPageIndex):
        """fill the one of the buffer with byte contents
        @param byteContents: byte contents of records
        @param bufferPageIndex: the ith buffer page
        """
        # for debug purpose
        if self._checkBound(bufferPageIndex):
            self.buffer[bufferPageIndex].overWrite(byteContents)
            # also fill in the in memory list with record objects
            self._parseRecordsFromPage(bufferPageIndex)
            self.fillIndicator[bufferPageIndex] = 1
    
    def _parseRecordsFromPage(self, bufferPageIndex):
        """return a list of *record objects* in page bufferPageIndex
        """
        bufferPage = self.buffer[bufferPageIndex]
        # fill the records list with readable records
        self.recordsPerPage[bufferPageIndex] = bufferPage.getAllRecords()

    def internalSortAndFlush(self, field, outputfile, offset):
        """
        sort all the pages inside the buffer
        """
        # get all the records from the buffer
        sortedRecords = sorted(sum(self.recordsPerPage, []))
        # fill the sorted records to each page in bytes
        outputfile.seek(offset, 0)
        
        start, end = 0, 0
        for i in range(0, self.bufferSize):
            # skip the buffer page where it has no contents at all, should not happens
            if not self.fillIndicator[i]:
                continue

            page = self.buffer[i]
            
            # get the number of records for each page
            end = len(self.recordsPerPage[i]) + start
            pageRecords = sortedRecords[start: end]

            # should be a size of a page or less
            writableRecords = b''.join([record.parseToBytes() for record in pageRecords])
            # overwrite the existing byte contents within the page with sorted record bytes
            byteContents = BitStream(writableRecords)
            page.overWrite(byteContents)

            # flush the sorted page to the outputfile
            self.flushPage(i, outputFile, outputFile.tell())
            start = end

    def recordsToBytes(self, records):
        """convert a list of records to a sequence of bytes
        return a in-memory bitstream
        """
        if self._checkFull():
            return BitStream(sum([record.parseToBytes() for record in records]))

    def sortAndFlushRuns(self, listOfRuns, outputFile, atOffset):
        """sort the listofRuns using the buffer and flush all of the pSize at a time
        @param listOfRuns : a list of run objects
        @param outputFile: the output file that the sorted run to be flushed
        """
        totalReads, totalWrites = 0, 0
        outputFile.seek(atOffset, 0)

        # NOTE: keeping sorting this set of runs when there are pages in some runs has not been marged
        allrecords = []
        for run in listOfRuns:
            while True:
                page = run.getNextPage()
                if page is None:
                    break
                allrecords.extend(page.getAllRecords())
                totalReads += 1
 
        # NOTE: for statistics, increment total disk access of number of pages in all the runs
        sortedRecords = sorted(allrecords)

        assert(len(listOfRuns) == len(self.buffer[:-1]))
        freeBuffers = []
        while any([not run.hasPassedLimits() for run in listOfRuns]):

            # insert each page from each run to the correct bucket if has it
            for i in range(0, len(listOfRuns)):
                if i in freeBuffers:


        # NOTE: each records is 64 bytes ==> must be divisible by 64
        outputPage = self.buffer[-1]
        outputPage.seek(0, 0)

        # flush the *full* output page
        for record in sortedRecords:
            if outputPage.overWrite(record.parseToBytes(), outputPage.tell()):
                self.flushPage(-1, outputFile, outputFile.tell())
                totalWrites += 1

        # flush the remaining records in the output page left by above iterations
        if outputPage.tell() > 0:
            self.flushPage(-1, outputFile, outputFile.tell())
            totalWrites += 1

        # flush all the buffer pages, including the output buffer page
        for i in range(0, self.bufferSize):
            self.emptyFlush(i)
        
        return totalReads, totalWrites

    def flushPage(self, ithPage, outputFile, atOffset):
        """write the contents of the ithpage to the disk"""
        self.buffer[ithPage].write(outputFile, atOffset)
        self.emptyFlush(ithPage)
        self.recordsPerPage[ithPage] = []

    def emptyFlush(self, ithpage):
        self.buffer[ithpage].flush()
        self.fillIndicator[ithpage] = 0

class Run:
    """represents each sorted run, contains multiple pages"""

    def __init__(self, pages):
        """a list of page objects"""

        # this list of pages are sorted
        self.pages = pages

        self.currentPointer = 0
        self.limit = len(pages)
    
    def runSize(self):
        return self.limit

    def getCurrentPointer(self):
        return self.currentPointer
    
    def incrementCurrentPointer(self):
        self.currentPointer += 1
    
    def hasPassedLimits(self):
        return self.currentPointer >= self.limit
    
    def getPage(self, pageIndex):
        return self.pages[pageIndex]
    
    def getNextPage(self):
        """get the next available page from this run"""
        current = self.currentPointer
        if self.currentPointer >= self.limit:
            return None
        result = self.pages[current]
        self.currentPointer += 1
        return result
    
    def getAllRecords(self):
        results = []
        for page in self.pages:
            results.extend(page.getAllRecords())
        return results
    
class ExternalSorter:
    """external sorting algorithm"""

    def __init__(self, numBufferPage, pSize, recordSize, sortField, inputFile, outputFile, dbFile, inputFileName, outputFileName):
        self.bufferSize = numBufferPage
        self.pSize = pSize
        self.buffer = Buffer(pSize, numBufferPage, sortField)
        self.recordSize = recordSize
        self.sortField = sortField

        # file pointers and reset location to be safe
        self.inputFile = inputFile
        self.inputFile.seek(0,0)

        self.outputFile = outputFile
        self.outputFile.seek(0,0)

        self.dbFilePath = dbFile
        # total number of pages need to be sorted

        self.totalPages = math.ceil(os.path.getsize(dbPath) / self.pSize)
        print(self.totalPages)

        self.ithRun = 0

        # extra file name constants
        self.inputName = inputFileName
        self.outputName = outputFileName

        # the statistics tracker
        self.statTracker = StatsTracker()

    def getStatTracker(self):
        return self.statTracker
    
    def externalSorting(self):

        '''pass 0 sorting'''
        self.inputFile.seek(0,0)
        self.outputFile.seek(0,0)

        # handle remaining pages
        rest = self.totalPages % self.bufferSize
        for i in range(0, self.totalPages, self.bufferSize):

            # NOTE: this is the last run for pass 0
            bufferSize = min(self.totalPages, self.bufferSize)
            if i + rest == self.totalPages:
                bufferSize = rest
            
            # NOTE: fill the contents from input file to the buffer page 'j'
            for j in range(0, bufferSize):
                self.inputFile.seek((i + j) * self.pSize, 0)
                byteContents = BitStream(self.inputFile.read(self.pSize))
                self.buffer.fillBuffer(byteContents, j)

                # NOTE: statistics collector
                self.statTracker.incrementPageRead()
                self.statTracker.incrementPageWrite()
            # perform internal sorting of the buffersize pages
            self.buffer.internalSortAndFlush(self.sortField, self.outputFile, self.outputFile.tell())
            
        # switch the role of inputfile and outputfile
        self.inputFile, self.outputFile = self.outputFile, self.inputFile
        sortedRuns = math.ceil(self.totalPages / self.bufferSize) # NOTE: get the number of sorted run after pass 0 are sorted
        
        # NOTE: statistic collector
        self.statTracker.incrementTotalPasses()

        '''sort and merge pass 1 and onward'''
        inputBufferSize = self.bufferSize - 1 # NOTE: leave the last buffer page as a output buffer
        pagesPerRun = self.bufferSize # NOTE: number of pages per run after pass 0

        # each iteration of the while loop represent a pass with multiple merging
        while sortedRuns > 1:
            
            # NOTE: statistic collector
            self.statTracker.incrementTotalPasses()

            # reset the pointer for the next pass and switch the role of input and output file
            self.inputFile.seek(0,0)
            self.outputFile.seek(0,0)

            # NOTE: Form run objects for the current pass, which contains multiple runs
            runs = self._constructRuns(sortedRuns, pagesPerRun)
            totalruns = len(runs)

            # NOTE: the size of the rolling window determine how many runs to be merge, assumes inputbuffersize >= len(runs)
            # sort by runs, each run has multiple pages
            rollingWindow = (0, min(inputBufferSize, totalruns))
            while rollingWindow[0] < rollingWindow[1]:
                # sort by the runs using the built in buffer
                reads, writes = self.buffer.sortAndFlushRuns(runs[rollingWindow[0]:rollingWindow[1]], self.outputFile, self.outputFile.tell())
                # update the next rolling window NOTE: the last part make sure does not goes out of range
                rollingWindow = (rollingWindow[1], rollingWindow[1] + min(inputBufferSize, totalruns - rollingWindow[1]))

                # NOTE: statistics collector
                self.statTracker.incrementPageRead(reads)
                self.statTracker.incrementPageWrite(writes)

            # update pages per run were sorted
            pagesPerRun *= inputBufferSize
            # number of sorted will be done after this pass
            sortedRuns = math.ceil(sortedRuns / inputBufferSize)
            # swap the role of input and output file
            self.inputFile, self.outputFile = self.outputFile, self.inputFile

        # TODO: rename the files the input file and the outputfile
        self.inputFile, self.outputFile = self.outputFile, self.inputFile
        return self.inputFile, self.outputFile

    def _constructRuns(self, sortedRuns, pagesPerRun):
        """construct a list of run to be sorted and merge, each run object contains multiple pages"""
        runs = []

        rest = self.totalPages - (sortedRuns - 1) * pagesPerRun
        pagesPerRun_temp = pagesPerRun # NOTE: the number of pages for each sorted run, change after each pass
        for i in range(0, self.totalPages, pagesPerRun):
            # NOTE: the last sorted run has less pages
            if i + rest == self.totalPages:
                pagesPerRun_temp = rest                            
            # NOTE: get a list of pages with Byte records for a single run
            runPages = []
            for j in range(0, pagesPerRun_temp):
                self.inputFile.seek((i + j) * self.pSize, 0) # always look from the beginning of the file
                runPages.append(RecordPage(self.pSize, BitStream(self.inputFile.read(self.pSize))))
            
            runs.append(Run(runPages))

        return runs

class StatsTracker:
    """statistics tracker for external sorting"""

    def __init__(self):
        self.pageRead = 0
        self.pageWrite = 0
        self.totalPasses = 0
    
    def getPageRead(self):
        return self.pageRead
    
    def getPageWrite(self):
        return self.pageWrite
    
    def getTotalPasses(self):
        return self.totalPasses

    def incrementPageRead(self, by=1):
        self.pageRead += by
    
    def incrementPageWrite(self, by=1):
        self.pageWrite += by
    
    def incrementTotalPasses(self, by=1):
        self.totalPasses += by

    def __repr__(self):
        """string representation of this statistic object"""
        return "(page reads:{} page writes:{} total passes:{})".format(self.pageRead, self.pageWrite, self.totalPasses)

if __name__ == "__main__":
    dbPath = "./names.db"
    inputFilename = "names.db"
    outputFilename = "output"
    dbFile = dbPath
    pageSizes = [512, 1024, 2048]
    Bs = [3, 10, 20, 50, 100, 200, 500, 1000, 5000, 10000]
    field = 1
    # redirects the outputs
    import sys
    outpuFile = open('./results/externalSorting_outputs.txt', 'w')
    sys.stdout = outpuFile
    for pageSize in pageSizes:
        for b in Bs:
            # the file names for different combinations
            outputDBname = outputFilename+"_pSize_"+str(pageSize)+"_Bs_"+str(b)+str(".db")
            # NOTE:make sure each combination of sorting has a clean startup
            cleanStartUp(inputFilename, outputDBname)
            inputFile = open("./{}".format(inputFilename), 'rb+')
            outputFile = open("./results/ExternalSorting_SortedFiles/{}".format(outputDBname), 'wb+')
            externalSorter = ExternalSorter(b, pageSize, RECORDSIZE, field, inputFile, outputFile, dbFile, inputFilename, outputFilename)
            inputFile, outputFile = externalSorter.externalSorting()
            inputFile.close()
            outputFile.close()
            # rename the file if the inputFile name is the output file
            if inputFile.name != dbPath:
                trueOutputFileName = inputFile.name
                trueInputFileName = outputFile.name
                temp = "temp"
                os.rename(inputFile.name, temp)
                os.rename(outputFile.name, trueOutputFileName)
                os.rename(temp, trueInputFileName)
            # handle statistics
            statTracker = externalSorter.getStatTracker()
            # output the followings to the file
            print("Page Size: {} - Buffer Size: {}".format(pageSize, b))
            print(statTracker)
            print("")
    outpuFile.close()
        

                




        

        
    

























