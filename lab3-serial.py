from mrjob.job import MRJob
from mrjob.step import MRStep
import os

class MatrixMultiplication(MRJob):

    def configure_args(self):
        super(MatrixMultiplication, self).configure_args()
        self.i = -1
        self.j =-1


    def mapper(self, _, line):
        # This function automatically reads in lines of code
        line = line.split()
        line = list(map(int, line))
        # if len(line)==2:
        #     rows, columns = line
        # else:
        row, col, value = line

        filename = os.environ['mapreduce_map_input_file']

        if filename == 'testMatrix.txt':
            yield col, (0, row, value)

        elif filename == 'testMatrix2.txt':
            yield row,  (1, col, value)

    def reducer_multiply(self, keys, values):
        matrix0=[]
        matrix1=[]
        matrixVal0=[]
        matrixVal1=[]
        print("key is: ")
        print(keys)

        for value in values:
            if value[0] == 0:
                print("key at start of 0 is: ")
                print(keys)
                print('append1: ', value)
                matrix0.append(value)
            elif value[0] == 1:
                print("key at start of 1 is: ")
                print(keys)
                print('append2: ',value)
                matrix1.append(value) 


        for row0,col0,val0 in matrix0:
            for row1,col1,val1 in matrix1:
                yield(col0, col1), val0*val1

    def changeKey(self, key, value):
        yield key, value

    def reducer_sum(self, key, values):
        yield key, sum(values)

    def steps(self): return [
        MRStep(mapper=self.mapper,
            reducer=self.reducer_multiply),
        MRStep( mapper = self.changeKey,
            reducer=self.reducer_sum)
        ]

if __name__ == '__main__':
    MatrixMultiplication.run()