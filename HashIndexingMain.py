from HashIndexing import *
from StaticHashing import *
from LinearHashing import *
from ExtendibleHashing import *

if __name__ == "__main__":
    pSize = 1024
    field = 0
    indexType = 1
    heapFilePath = "./names.db"
    numBuckets = 512

    import os
    byteSize = os.path.getsize(heapFilePath)

    # redirects the output to the file
    import sys
    outpuFile = open('./results/part2_outputs.txt', 'w')
    sys.stdout = outpuFile

    # Construct all three index files
    staticIndex = staticHashingIndexer("staticIndex", numBuckets, pSize, field, indexType, DATAENTRYSIZE)
    extendibleIndex = ExtendibleHashingIndexer("extendibleIndex", numBuckets, pSize, field, indexType, DATAENTRYSIZE)
    linearHashIndex = LinearHashingIndexer("linearHashIndex", numBuckets, pSize, field, indexType, DATAENTRYSIZE)

    # create all three index files by hashing each records from names.db file
    for index in [extendibleIndex]:
        outpuFile = open('./results/IndexFiles/part2_outputs_{}.txt'.format(index.indexFileName), 'w')
        sys.stdout = outpuFile
        with open(heapFilePath, 'rb') as heapfile:
            rowid = 0
            while True:
                heapfile.seek(RECORDSIZE * rowid, 0)
                if heapfile.tell() >= byteSize:
                    break
                # Read the records with null characters behind
                firstName = heapfile.read(FIRSTNAMESIZE).decode("utf-8")
                index.hash_(firstName, rowid)
                rowid += 1
        # write the index file to disk at path ./results/IndexFiles/
        statsTracker = index.printStatistics()
        statsTracker.plotHistogram(index.indexFileName)
        index.writeToDisk()
        outpuFile.close()