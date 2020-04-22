inputFile = open('columns.txt', 'r')

splits = inputFile.readline().split(',')
inputFile.close()

i = 0
output = ''
for i in range(len(splits)):
    output += str(i) + ' ' + splits[i].strip() + '\n'
    i += 1

outputFile = open('columns-w-indexes.txt', 'a')
outputFile.write(output)

outputFile.flush()
outputFile.close()


