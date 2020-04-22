inputFile = open('columns.txt', 'r')

splits = inputFile.readline().split(',')
inputFile.close()

i = 0
output = ''
java_output = ''
for i in range(len(splits)):
    output += str(i) + ' ' + splits[i].strip() + '\n'
    java_output += 'public static final String ' + splits[i].strip().replace(' ', '_').upper() + ' = "' + splits[i] + '";\n'

outputFile = open('java-outputs.txt', 'a')
outputFile.write(java_output)

outputFile.flush()
outputFile.close()


