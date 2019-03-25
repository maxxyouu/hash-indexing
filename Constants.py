numPasses = 0
numPageRead = 0
numPageWrite = 0


RECORDSIZE = 64
FIRSTNAMESIZE = 12
LASTNAMESIZE = 14
EMAILSIZE = 38
dbPath = "C:\\Users\\Max You\\Desktop\\COURSES\\CSC443\\CSC443_W2019_A2\\names.db"
DATAENTRYSIZE = 3 + 12 # 12 bytes for FIRSTNAME, 3 bytes for rowid

ZERO = "0"
ONE = "1"
BYTESIZE = 8
# 2 bytes ==> 16 bits ==> 2^16 alot of pages
PAGE_POINTER_SIZE = 2
TOTAL_RECORDS =500000

# each metada occupies 8 bytes
METADATA_SIZE = 8
PAGE_THREHOLD_SIZE = METADATA_SIZE
OVERFLOW_DIR_POINTER_SIZE = 2
OVERFLOW_BUCKET_POINTER_SIZE = 2
DEPTH_SIZE = 2

# record informaiotn
RECORDSIZE = 64
FIRSTNAMESIZE = 12
LASTNAMESIZE = 14
EMAILSIZE = 38

# num of records per page size in bytes
RECORD_METADATA = 2