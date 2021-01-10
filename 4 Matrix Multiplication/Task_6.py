# To run inline:
# python "4 Matrix Multiplication\Task_6.py" "4 Matrix Multiplication\A.txt" "4 Matrix Multiplication\B.txt" > "4 Matrix Multiplication\C.txt"

# Due to mapper_raw cannot be run on a local cluster!

from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np

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
    def combine_tuples(row_column, name_row_or_col_valuelist):
        """
        This combiner does not change anything drastically, it almost increases the time it takes to run jobs by a
        factor of ~1.5 but it decreases the amount of data sent to the reducers by ~1/3. So while sacrificing time
        performance, it will reduce redundant transfer in an actual DFS. Note that when running this program on one
        machine that without this combiner more RAM is necessary when initialising the reducer. This is because input
        sent to the reducer is  sorted in memory, this might cause a 'MemoryError' when the system does not have
        sufficient RAM to process large matrices.
        """
        yield row_column, name_row_or_col_valuelist

    @staticmethod
    def calculate_dot(row_column, name_row_or_col_valuelist):
        """
        MrJob automatically sorts the values of identical keys when passing the data to a reducer,
        so the values for a 'row_column' key will already be in the following sorted order:
        [[matrix1 name, 0, value], [matrix1 name, 1, value], ..., [matrix1 name, n, value], [matrix2 name, 0, value],
        [matrix2 name, 1, value], ..., [matrix2 name, n, value]]
        """
        row_column = tuple(row_column)
        name_row_or_col_valuelist = tuple(name_row_or_col_valuelist)
        # The common size of the two matrices, also equal to MRJob.matrix2[1][0] and len(name_row_or_col_valuelist)//2
        common_size = MRJob.matrix1[1][1]

        # Get the values belonging to the matrix with the 'smallest' name (in sorted order), note that this can be
        # either matrix1 or matrix2
        matrixA_values = [value for name_row_or_col_value in name_row_or_col_valuelist
                          for name, row, value in name_row_or_col_value][0:common_size]
        # Get the values belonging to the matrix with the 'highest' name (in sorted order based on 'row_or_col')
        # note that this can be either matrix1 or matrix2
        matrixB_values = [value for name_row_or_col_value in name_row_or_col_valuelist
                          for name, row, value in name_row_or_col_value][common_size::]
        # As the tuples were sorted based on their row/column we know they are in the same order so we can zip them,
        # and take the sum of the products to obtain the value for that specific 'row_column'
        value = sum(val1*val2 for val1, val2 in zip(matrixA_values, matrixB_values))
        yield row_column, value

    def steps(self):
        return [
            MRStep(mapper_raw=self.read_matrix),
            # We need to perform this in two distinct steps, as the dimensions of both matrices need to be known
            # the first step is negligible in terms of time consumption, so this will not cause any performance issues
            MRStep(mapper=self.generate_tuples,
                   combiner=self.combine_tuples,
                   reducer=self.calculate_dot)
        ]


if __name__ == '__main__':
    MatrixMultiplication.run()
