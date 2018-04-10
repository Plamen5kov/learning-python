def write_to_file(file, content):
    file = open(file, "a") 
    file.write(content)
    file.close()

CHUNCK_SIZE = 4 * 1024 * 1024
def read_in_chunks(inputFile, chunk_size=CHUNCK_SIZE):
    list = []
    eofReached = False
    currentReadSize = 0
    while True:
        data = inputFile.readline().strip()

        if not data:
            eofReached = True
            yield (list, eofReached)
            break

        currentReadSize += len(data)
        list.append(data)
        if currentReadSize >= chunk_size:
            yield (list, eofReached)
            list.clear()
            currentReadSize = 0