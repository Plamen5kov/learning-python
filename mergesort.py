import fileoperations
import uuid
from os import listdir, remove
from os.path import isfile, join

def sort_input_file(inputFilePath, outputDir, availableMemory, chunkFileSize):
    canLoadInMemory = availableMemory / chunkFileSize / 2
    sort_file_chunks(inputFilePath, outputDir)
    mergeSortedFiles(outputDir, canLoadInMemory, chunkFileSize)

def sort_file_chunks(inputFilePath, outputDir):
    inputFile = open(inputFilePath)
    counter = 1
    fileIndex = 0

    for list, eofReached in fileoperations.read_in_chunks(inputFile):

        list.sort()
        sortedFileContent = '\n'.join(list).strip()
        fileoperations.write_to_file( outputDir + "file" + str(counter) + ".txt", sortedFileContent)
        counter += 1
        fileIndex += 1

        if eofReached:
            break

    inputFile.close()

def mergeSortedFiles(outputDir, canLoadInMemory, chunkFileSize):
    files = [f for f in listdir(outputDir) if isfile(join(outputDir, f))]
    
    if len(files) == 1:
        return

    loadedFilesIndex = 0
    filesToMerge = []

    index = 0
    for currentFile in files:
        filesToMerge.append(join(outputDir, currentFile))
        loadedFilesIndex += 1
        if loadedFilesIndex >= canLoadInMemory or index == (len(files)-1):
            loadedFilesIndex = 0
            mergeFiles(filesToMerge, chunkFileSize, outputDir)
            filesToMerge = []
        index += 1

    mergeSortedFiles(outputDir, canLoadInMemory, chunkFileSize)

def mergeFiles(filesToMerge, chunkFileSize, outputDir):
    inMemorySortedFiles = {}#make(map[int]*list.List)
    openFileHandles = {}#make(map[int]streamHandler)

    comparedTupleElements = {}#make(map[int]fileCompareHandler)
    index = 0
    for currentFile in filesToMerge:
        if currentFile != "":
            index += 1

			#open handles on files that should be merged
            fileHandle = open(currentFile)

			# load initial file buffers
            list, eof = fileoperations.read_in_chunks(fileHandle).__next__()
            inMemorySortedFiles[index] = list
            listIterator = iter(list or [])
            firstElement = listIterator.__next__()

			# save opened handles so we can close them later on
            
            comparedTupleElements[index] = [firstElement, listIterator]
            openFileHandles[index] = (fileHandle, currentFile)

            if eof:
                continue

    sortedFileContentBuffer = []#make([]string, chunkFileSize)
    bufferCounter = 0
    outputFile = join(outputDir, uuid.uuid4().hex + ".txt")
    
    while True:
        minElement, filesEndReached = chooseMinElement(inMemorySortedFiles, comparedTupleElements, openFileHandles, chunkFileSize)
        
        if minElement == "" or filesEndReached:
            flushToFile(sortedFileContentBuffer, chunkFileSize, bufferCounter, outputFile)
            bufferCounter = 0
            break
        elif bufferCounter > chunkFileSize:
            flushToFile(sortedFileContentBuffer, chunkFileSize, bufferCounter, outputFile)
            bufferCounter = 0

        sortedFileContentBuffer.append(minElement)
        bufferCounter += len(minElement)

    #close open file handles and delete merged files
    for index in openFileHandles:
    	openFileHandles[index][0].close()
    	remove(openFileHandles[index][1])

def flushToFile(sortedFileContentBuffer, chunkFileSize, bufferCounter, outputFile):
    content = '\n'.join(sortedFileContentBuffer).strip()
    fileoperations.write_to_file(outputFile, content)
    sortedFileContentBuffer.clear()

def chooseMinElement(inMemorySortedFiles, comparedTupleElements, openFileHandles, chunkFileSize):
    minElement = ""
    minFileIndex = -1
    filesEndReached = True
    for currentFileIndex in comparedTupleElements:
        currentValue = comparedTupleElements[currentFileIndex][0]

		# if current element is empty try to lazy load more values from file
        if currentValue == "":
            list, eofReached = fileoperations.read_in_chunks(openFileHandles[currentFileIndex][0]).__next__()
            if eofReached and len(list) <= 0:
                continue

            comparedTupleElements[currentFileIndex][1] = iter(list)
            pushNextElementToTuple(comparedTupleElements, currentFileIndex, True)

        if comparedTupleElements[currentFileIndex][0] != "":
            filesEndReached = False
            if comparedTupleElements[currentFileIndex][0] < minElement or minElement == "":
                minElement = comparedTupleElements[currentFileIndex][0]
                minFileIndex = currentFileIndex

    pushNextElementToTuple(comparedTupleElements, minFileIndex, False)
    return (minElement, filesEndReached)

def pushNextElementToTuple(comparedTupleElements, index, takeFront):
    if index == -1:
        return
    try:
        comparedTupleElements[index][0] = comparedTupleElements[index][1].__next__()
    except StopIteration:
        comparedTupleElements[index][0] = ""
