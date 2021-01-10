import numpy as np

A = np.loadtxt('A.txt')
B = np.loadtxt('B.txt')

C_matmul = np.matmul(A, B)
C_mrjob = np.zeros((C_matmul.shape))

with open('C.txt', 'r') as C_file:
    for line in C_file:
        row, col, value = line.split(',')
        C_mrjob[int(row), int(col)] = float(value)

print(np.linalg.norm(C_mrjob-C_matmul))