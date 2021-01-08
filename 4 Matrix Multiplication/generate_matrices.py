import numpy as np

# generate A and B matrices
ROW_A = 100
MUTUAL = 50
COL_B = 200
A = np.random.randint(-10, 10, size=(ROW_A, MUTUAL))
B = np.random.randint(-10, 10, size=(MUTUAL, COL_B))
C = np.matmul(A, B)
np.savetxt('Asmall.txt', A)
np.savetxt('Bsmall.txt', B)
np.savetxt('Csmallcorrect.txt', C)

Abig = np.loadtxt('A.txt')
Bbig = np.loadtxt('B.txt')

np.savetxt('Ccorrect.txt', np.matmul(Abig, Bbig))
