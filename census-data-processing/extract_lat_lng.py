import csv
import sys

# years = [str(x) for x in range(2014, 2018)]
years = ['2015']

dataDir = '../dataset'

for year in years:
    lats = []
    lngs = []
    filePath = dataDir + '/' + year + '.csv'
    file = open(filePath, 'r')
    csv_file = csv.reader(file, delimiter=',')
    header = True
    for row in csv_file:
        if header:
            header = False
            continue
        lat = row[43]
        lng = row[44]
        print("lat", lat)
        print("lng", lng)
        lats.append(lat)
        lngs.append(lng)
        sys.exit(0)

    print('len(lats):', len(lats))
    print('len(lngs):', len(lngs))

    assert len(lats) == len(lngs)
    
    output = []
    n = len(lats)
    for i in range(n):
        output.append(lats[i] + ',' + lngs[i] + '\n')
        print(output[i])

    sys.exit(0)
    
    outputFile = open(year + '.csv', 'a')
    outputFile.write(output)
    outputFile.flush()
    outputFile.close()
