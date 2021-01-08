from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import numpy as np
import operator

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f"{key[0]}, {key[1]}, {value}", 'utf-8')

class MatrixMultiplication(MRJob):
    MRJob.matrix1 = ()
    MRJob.matrix2 = ()
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    @staticmethod
    def read_matrix(path, uri):
        matrix = np.loadtxt(path)  # path is locally accessible
        name = uri.split('\\')[-1] # Get the filename
        if not MRJob.matrix1:  # check whether matrix1 already has a value
            MRJob.matrix1 = (name, matrix.shape)  # set it equal to the filename along with the matrix dimensions
        else:  # matrix1 already exists
            # amount of columns in matrix1 are equal to rows in matrix2, we can do matrix1 * matrix2
            if MRJob.matrix1[1][1] == matrix.shape[0]:
                MRJob.matrix2 = (name, matrix.shape)
            else:
                assert matrix.shape[1] == MRJob.matrix1[1][0], 'Matrices not multiplicable'
                MRJob.matrix2 = MRJob.matrix1  # We'll always do matrix1 * matrix2
                MRJob.matrix1 = (name, matrix.shape)
            MRJob.matrix2 = (name, matrix.shape)
        for row_index, row in enumerate(matrix):
            for column_index, element in enumerate(row):
                yield (name, row_index, column_index, element), None

    @staticmethod
    def generate_tuples(info, _):
        name, row_index, column_index, value = tuple(info)
        if name == MRJob.matrix1[0]:  # tuple is from matrix1
            for column in range(MRJob.matrix2[1][1]):  # For each column in matrix2
                yield (row_index, column), (name, column_index, value)
        else:  # tuple is from matrix2
            for row in range(MRJob.matrix1[1][0]):  # For each row in matrix1
                yield (row, column_index), (name, row_index, value)

    @staticmethod
    def calculate_dot(row_column, name_row_or_col_valuelist):
        row_column = tuple(row_column)
        name_row_or_col_valuelist = tuple(name_row_or_col_valuelist)
        # Sort the lists based on their row/column index
        name_row_or_col_valuelist = sorted(name_row_or_col_valuelist, key=operator.itemgetter(1))
        # Get the values belonging to matrix1
        matrix1_values = [value for name, col, value in name_row_or_col_valuelist if name == MRJob.matrix1[0]]
        # Get the values belonging to matrix2
        matrix2_values = [value for name, row, value in name_row_or_col_valuelist if name == MRJob.matrix2[0]]
        # As the tuples were sorted based on their row/column we know they are in the same order so we can zip them
        value = sum(val1*val2 for val1, val2 in zip(matrix1_values, matrix2_values))
        yield row_column, value

    def steps(self):
        return [
            MRStep(mapper_raw=self.read_matrix),
            # We need to perform this in two distinct steps, as the dimensions of both matrices need to be known
            # the first step is negligible in terms of time consumption, so this will not cause any performance issues
            MRStep(mapper=self.generate_tuples,
                   reducer=self.calculate_dot)
        ]


if __name__ == '__main__':
    start = time.time()
    MatrixMultiplication.run()
    total_time = int(time.time() - start)
    print(f'Took {total_time//60} minutes and {total_time%60} seconds to finish')
