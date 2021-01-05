from mrjob.job import MRJob
from mrjob.step import MRStep
import os
import time


# [i,j]*[j,k] -> [i,k]
class MRMatrixMultiplication(MRJob):
    output_file = open('C_MJ.txt', 'w')  # resulting file

    def mapper(self, _, line):
        """
        this mapper yields every element of the matrix with a corresponding key j which is common to both matrices
        :param _: None
        :param line: one line from the input file
        :return: (col, (A, row, value) or (row, (B, col, value)
        """
        line = list(map(int, line.split()))  # string to array of integers
        row, col, value = line

        file_name = os.environ['mapreduce_map_input_file']  # get the name of the map input file
        if file_name == 'file://A.txt':
            # self.output_file2.write(str(row) + str(('A', col, value)) + '\n')
            yield col, ('A', row, value)  # key is column as it is j in matrix A
        elif file_name == 'file://B.txt':
            # self.output_file2.write(str(row) + str(('B', col, value))+'\n')
            yield row, ('B', col, value)  # key is row as it is j in matrix B

    def reducer_multiply(self, _, values):
        """
        this reducer performs every pairwise multiplication and sends it to the final reducer.
        :param _: None
        :param values: elements of both matrices
        :return: ((row_a, col_b), val_a * val_b)
        """
        matrixA = []
        matrixB = []
        for value in values:
            if value[0] == 'A':
                matrixA.append(value)
            elif value[0] == 'B':
                matrixB.append(value)

        for col_a, row_a, val_a in matrixA:
            for row_b, col_b, val_b in matrixB:
                yield (row_a, col_b), val_a * val_b

    def reducer_sum(self, key, values):
        """
        this final reducer gets the sum of all the multiplications per resulting matrix element
        :param key: (i,k) i is a row and k is a column in a resulting matrix
        :param values: each item of values is a result of multiplication of element A[i,j] on element B[j,k],
        :return: (key=entry index in resulting matrix, value=value for the corresponding position)
        """
        final_sum = sum(values)
        c_i = key[0]  # resulting matrix row
        c_k = key[1]  # resulting matrix col
        self.output_file.write(str(c_i) + " " + str(c_k) + " " + str(final_sum) + "\n")
        yield key, final_sum

    def steps(self):  # two-step job
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_multiply),
            MRStep(reducer=self.reducer_sum)
        ]


if __name__ == '__main__':
    start = time.time()
    MRMatrixMultiplication.run()
    print("Multiplication performed in {} seconds(including writing)".format(time.time() - start))

# python3 task_6.py A.txt B.txt
