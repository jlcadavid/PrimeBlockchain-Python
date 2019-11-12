import math as m

def isPrime(n):
	sw = True
	if(n < 2): return False
	j = 2
	while( j <=m.sqrt(n) and sw):
		if n% j == 0:
			sw = False
		j+=1
	return sw 