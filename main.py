import numpy as np
from mpi4py import MPI
import math
import time
import datetime
import copy
import hashlib

import isPrimeScript

import primeBlockchain as blockchain

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

arr = []
res = []
c = 0

if rank == 0:

    print('Digite W en cientos de miles')
    W = int(input())
    maxK = W * 100000
    start_time = time.time()
    arraySize = int(math.ceil((maxK/float(size-1))/float(2)))
    for i in range(1, size):
        comm.Send([np.array([maxK, arraySize]), MPI.INT], dest=i)

    primes = [2]
    blockchain.MinimalChain()

    res = np.empty([arraySize, 2], np.int32)
    check = np.array([0])
    missing = size - 1
    while missing > 0:
        comm.Recv([res, MPI.INT], tag=1)
        #comm.Recv([check, MPI.INT], tag=2)
        #missing = missing - check
        missing = missing - 1
        for p in range(0, arraySize):
            if res[p][1] == 1 and res[p][0] != 0:
                primes.append(res[p])
                blockchain.MinimalChain.add_block(res[p])

    end_time = time.time()
    #print('done!')
    print('tenemos > ' + str(len(primes)) + ' primos')
    print('time > ' + str(end_time-start_time))
else:
    data = np.array([0, 0])
    comm.Recv([data, MPI.INT], source=0)

    jump = (size-1)*2
    cur = (2 * rank) + 1
    c = 0

    #print(str(rank) + ' reportandose, con un init de ' + str(cur) + ' y un jump de ' + str(jump))

    pr = np.empty([data[1], 2], np.int32)
    while cur < data[0]:
        pr[c][0] = cur
        pr[c][1] = 1 if isPrimeScript.isPrime(cur) else 0
        c = c + 1
        cur = cur + jump

        #if c == arraySize:
            #comm.Send([pr, MPI.INT], dest=0, tag=1)
            #comm.Send([np.array([0]), MPI.INT], dest=0, tag=2)
            #pr = np.empty([arraySize, 2], np.int32)
            #c = 0

        #comm.Send(np.array([1 if isPrimeScript.isPrime(cur) else 0]), dest=0, tag=cur)
        #comm.Send(np.array([0]), dest=0, tag=cur+1)

    comm.Send([pr, MPI.INT], dest=0, tag=1)
    #comm.Send([np.array([1]), MPI.INT], dest=0, tag=2)

    #print(str(rank) + ' reportandose de nuevo, con un conteo de ' + str(c) + ' de los ' + str(data[0]) + ' totales')