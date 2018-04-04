import os
from mrjob.job import MRJob
from mrjob.step import MRStep

inputfile = open('File1ForLab3.txt','r+')
linesOfFile=inputfile.readlines()
noOfRows, noOfCols = linesOfFile[0].split()
inputfile.seek(0)
inputfile.writelines(linesOfFile[1:])
inputfile.truncate()
inputfile.close()

class MatrixMultiplication(MRJob):

    f = open('OutputAlgorithmB.txt', 'w')

    def mapper(self, _, line):
        # This function automatically reads in lines of code
        line = line.split()
        line = list(map(int, line))
        row, col, value = line

        for i in range(0,int(noOfCols)):
            yield (row, i), (0, col, value)

        for j in range(0, int(noOfRows)):
            yield (j, col), (1, row, value)

    def reducer_multiply(self, keys, values):
        matrix0=[]
        matrix1=[]
        matrixVal0=[]
        matrixVal1=[]
        print ("key is: ")
        print(keys)
        for value in values:
            if value[0] == 0:
                print('append1: ', value)
                matrix0.append(value)
            elif value[0] == 1:
                print('append2: ',value)
                matrix1.append(value) 
        matrix0= sorted(matrix0, key=lambda x: x[1])
        matrix1= sorted(matrix1, key=lambda y: y[1])

        for row0,col0,val0 in matrix0:
            for row1,col1,val1 in matrix1:
                if col0 == col1:
                    yield keys, val0*val1

    def reducer_sum(self, key, values):
        total = sum(values)
        yield key, total
        x = key[0]
        y = key[1]
        self.f.write(str(x) + " " + str(y) + " ")
        self.f.write(str(total) + "\n")

    def steps(self): return [
        MRStep(mapper=self.mapper,
            reducer=self.reducer_multiply),
        MRStep(reducer=self.reducer_sum)
        ]

if __name__ == '__main__':
    MatrixMultiplication.run()

    inputfile = open('File1ForLab3.txt','r+')
    linesOfFile=inputfile.readlines()

    string1 = str(noOfRows)+ " "+ str(noOfCols) + '\n'
    linesOfFile.insert(0, string1)

    inputfile.seek(0)
    inputfile.writelines(linesOfFile)
