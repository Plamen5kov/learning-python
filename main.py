import fileoperations
import mergesort

inputFilePath = './input/bigfile.txt'
outputFilePath = './out/'
availableMemory = 70 * 1024 * 1024 #the hard drive works with 35mb/s read/write so about 70mb of operational memmory should be close to optimal
chunkFileSize = 5 * 1024 * 1024

mergesort.sort_input_file(inputFilePath, outputFilePath, availableMemory, chunkFileSize)

