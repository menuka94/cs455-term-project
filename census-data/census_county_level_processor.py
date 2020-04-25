years = [str(x) for x in range(2014,2018)]
aspect = 'poverty'

for year in years:
    file = open('acs5_' + year + '_' + aspect + '_counties.csv')
    filtered_lines = []
    line = file.readline()
    header = True
    while line != "":
        if header:
            filtered_lines.append(line)
            header = False
        if "new york county, new york" in line.lower() or \
            "richmond county, new york" in line.lower() or \
                "queens county, new york" in line.lower() or \
                    "kings county, new york" in line.lower() or \
                        "bronx county, new york" in line.lower():
            filtered_lines.append(line)
        line = file.readline()
    
    outputFile = open(year + '_ny_' + aspect + '_counties.csv', 'w')
    outputFile.write(''.join(filtered_lines))
    outputFile.flush()
    outputFile.close()
    