import numpy as np

rows  = 20
cols = 20
low = 0
high = 99
step = 1

matrix = np.random.choice([x for x in xrange(low,high,step)],rows*cols)
matrix.resize(rows,cols)

print(matrix)

thisThing = open('thisFile.txt', 'w+')
thisThing.write(str(rows)+' '+str(cols)+ '\n')

for i in range(0,rows):
    for j in range(0,cols):
        # print matrix[i][j]
        # print i
        # print j
        # print ''
        thisThing.write(str(i) + ' ' + str(j) + ' ' + str(matrix[i][j]) + '\n')


thisThing.close()