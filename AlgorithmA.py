from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import time

t00=time.time()
inputfile = open('outA3.list','r+')
linesOfFile=inputfile.readlines()
noOfRows, noOfCols = linesOfFile[0].split()

inputfile2 = open('outB3.list','r+')
linesOfFile2=inputfile2.readlines()
noOfRows2, noOfCols2 = linesOfFile2[0].split()

counter = 0

class MatrixMultiplication(MRJob):

    f = open('OutputAlgorithmA.txt', 'w')

    def mapper(self, _, line):
        global counter
        # This function automatically reads in lines of code
        line = line.split()
        line = list(map(int, line))
        if len(line) == 3:
            row, col, value = line
        elif len(line) == 2 and counter == 1:
            row, value = line
            col = 0
        elif len(line) == 2 and counter == 0:
            counter = 1
            return

        filename = os.environ['mapreduce_map_input_file']

        if 'A' in filename:
            yield col, (0, row, value)
        elif 'B' in filename:
            yield row,  (1, col, value)

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
    t0 = time.time()
    MatrixMultiplication.run()
    t1 = time.time()

    totalWithoutWrite = t1 - t0
    print ("Total time for algorithmA: " + str(totalWithoutWrite))
    #print ("Total time for algorithmA with writing to file: " + str(totalWithWrite))