inputfile = open('outNetwork.list','r+')
linesOfFile=inputfile.readlines()
noOfRows, noOfCols = linesOfFile[0].split()

ouputmatrix = open('newinputQ6.txt', 'w')
matrix_construct = [ [0] * int(noOfCols) for index in range(int(noOfRows))]
for line in linesOfFile[1:]:
    row, column, value = line.split()
    matrix_construct[int(row)][int(column)] = value

for i in range(int(noOfRows)):
    for j in range(i, int(noOfCols)):
        matrix_construct[j][i] = matrix_construct[i][j]

for i in range(int(noOfRows)):
    for j in range(int(noOfCols)):
        output = str(i) + ' ' + str(j) + ' ' + str(matrix_construct[i][j]) + '\n'
        ouputmatrix.writelines(output)

inputfile.close()
ouputmatrix.close()