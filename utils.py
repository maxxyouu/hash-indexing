def cleanStartUp(inputFilename, outputFilename):
    import os
    #make sure each time a fresh starts, assume the input file exists
    if os.path.isfile(inputFilename):
        os.remove(inputFilename)
    from shutil import copyfile
    copyfile("../{}".format(inputFilename), "./{}".format(inputFilename))  # copy the input file from the parent directory to local
    # if the output file exists, remove it
    if os.path.isfile(outputFilename):
        os.remove(outputFilename)
    
def byte_to_int(bytes_):
    return int.from_bytes(bytes_, byteorder="big")

def int_to_byte(num, fixedByteSize=0):
    """this also make sure byte aligned"""
    bytes_ = len(format(num, 'b'))
    # make the number in a fixed byte size
    if fixedByteSize > 0 and bytes_ <= (fixedByteSize * 8):
        bytes_ = fixedByteSize
    else:
        bytes_ += (bytes_ % 8)
    return (num).to_bytes(bytes_, "big")

def int_to_bin(decimal, binSize):
    """
    convert the decimal into binary string with binSize binary code
    """
    bin = format(decimal, 'b')

    if len(bin) < binSize:
        return ('0'*(binSize-len(bin))) + bin
    return bin