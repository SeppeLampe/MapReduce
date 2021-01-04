import numpy as np
# [i,j]*[j,k] -> [i,k]

# generate A and B matrices
A = np.random.randint(-10, 10, size=(1000, 50))
B = np.random.randint(-10, 10, size=(50, 10))
i_a, j_a = A.shape
i_b, j_b = B.shape

with open('A.txt', 'w') as f:
    for i in range(i_a):
        for j in range(j_a):
            f.write("{} {} {}\n".format(i, j, A[i, j]))
    f.close()
with open('B.txt', 'w') as f2:
    for i in range(i_b):
        for j in range(j_b):
            f2.write("{} {} {}\n".format(i, j, B[i, j]))
    f2.close()

# C = np.dot(A, B)
# np.savetxt('C_orig.txt', C)

