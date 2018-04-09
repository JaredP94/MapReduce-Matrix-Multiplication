import numpy as np

rows  = 3
cols = 3
low = 0
high = 10
step = 1

matrix = np.random.choice([x for x in xrange(low,high,step)],rows*cols)
matrix.resize(rows,cols)

print(matrix)

thisThing = open('File1ForLab3.txt', 'w+')
thisThing.write(str(rows)+' '+str(cols)+ '\n')

for i in range(0,rows):
    for j in range(0,cols):
        if i == j:
            matrix[i][j] = 0
        thisThing.write(str(i) + ' ' + str(j) + ' ' + str(matrix[i][j]) + '\n')
        
thisThing.close()