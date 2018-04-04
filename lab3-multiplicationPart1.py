from mrjob.job import MRJob
from mrjob.step import MRStep
import os

inputfile = open('File2ForLab3.txt','r+')
linesOfFile=inputfile.readlines()
noOfRows, noOfCols = linesOfFile[0].split()
inputfile.seek(0)
inputfile.writelines(linesOfFile[1:])
inputfile.truncate()
inputfile.close()

class MatrixMultiplication(MRJob):

    f = open('partialOutputQ6.txt', 'w')

    def mapper(self, _, line):
        # This function automatically reads in lines of code
        line = line.split()
        line = list(map(int, line))
        row, col, value = line

        yield col, (0, row, value)
        yield row, (1, col, value)

    def reducer_multiply(self, keys, values):
        matrix0=[]
        matrix1=[]
        matrixVal0=[]
        matrixVal1=[]

        for value in values:
            if value[0] == 0:
                matrix0.append(value)
            elif value[0] == 1:
                matrix1.append(value) 

        for row0,col0,val0 in matrix0:
            for row1,col1,val1 in matrix1:
                yield(col0, col1), val0*val1

    def changeKey(self, key, value):
        yield key, value

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
        MRStep( mapper = self.changeKey,
            reducer=self.reducer_sum)
        ]

if __name__ == '__main__':
    MatrixMultiplication.run()