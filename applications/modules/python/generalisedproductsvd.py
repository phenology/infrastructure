#!/usr/bin/env python3

"""

Functions
---------

"""

import numpy
import scipy.linalg
from sklearn.preprocessing import normalize

def preprocess(*dataSets, p = None, center = True, normalize = True):
	Ps = []
	Ss = []
	for i in range(len(dataSets)):
		if center: # Center before or after PCA?
			dataSets[i] = dataSets[i] - dataSets[i].mean(axis = 0)
		if normalize: # Normalize before or after PCA?
			dataSets[i] = normalize(dataSets[i])
		if p is not None and 0 < p < min(dataSets[i].shape):
			U, s, Vt = scipy.linalg.svd(dataSets[i], full_matrices = False)
			U = U[:, :p]
			s = s[:p]
			Vt = Vt[:p]
			Ps[i] = U
			Ss[i] = numpy.diag(sA) @ VAt
		else: # Check if really faster for p = None
			Ps[i], Ss[i] = scipy.linalg.qr(A, mode = "economic")
	return Ps, Ss

def postprocess(s, Vp, Ps, q = None):
	for i in range(len(Vp)): # Ps @ Vp?
		V = Ps[i] @ Vp
	if q is not None and 0 < q < len(s):
		s = s[:q]
		for j in range(len(V)):
			V[j] = V[j][:, :q]
	return s, V

def kettenringmatrix(Ss):
	r = len(Ss)
	m = min(Ss[0].shape)
	rows = []
	for j in range(len(Ss)):
		cells = []
		for k in range(len(Ss)):
			if j == k:
				cells[k] = numpy.zeros(Ss[j].shape)
			else:
				cells[k] = Ss[j].Ss[k].T
		rows[j] = numpy.concatenate(cells, axis = 1)
	K = numpy.concatenate(rows, axis = 0)
	return K, r, m

def kettenringsvd(K, r, m):
	l, v = scipy.linalg.eig(K)
	l = l[::r]
	argsort = numpy.flip(numpy.argsort(l), 0)
	l = l[argsort]
	l = l[:m]
	s = l
	v = v[:, ::r] # or !length?
	v = v[:, argsort]
	V = []
	for l in range(r):
		Vn = normalize(v[(m * l):(m * (l + 1)), :m])
	return s, V

def gcca(*dataSets, p = None, q = None, center = True, normalize = True):
	# Add validation
	Ps, Ss = preprocess(dataSets, p, center, normalize)
	K, r, m = kettenringmatrix(Ss)
	s, Vp = kettenringsvd(K, r, m)
	s, V = postprocess(s, Vp, Ps, q)
	return s, V

def svd2(A): # Only for testing, remove later
	m = A.shape[0]
	n = A.shape[1]
	zeros1 = numpy.zeros((m, m))
	zeros2 = numpy.zeros((n, n))
	row1 = numpy.concatenate((zeros1, A), axis = 1)
	row2 = numpy.concatenate((A.T, zeros2), axis = 1)
	B = numpy.concatenate((row1, row2), axis = 0)
	l, v = scipy.linalg.eig(B)
	l = l[::2]
	argsort = numpy.flip(numpy.argsort(l), 0)
	l = l[argsort]
	l = l[:n]
	v = v[:, ::2]
	v = v[:, argsort]
	U = normalize(v[:m, :n])
	V = normalize(v[m:, :n])
	s = l
	return U, s, V.T
	
def svd3(A, B, C): # Only for testing, remove later
	mA = A.shape[0]
	nA = A.shape[1]
	mB = B.shape[0]
	nB = B.shape[1]
	mC = C.shape[0]
	nC = C.shape[1]
	if not (nA == nB == nC):
		print("Error!")
	zeros1 = numpy.zeros((mA, mA))
	zeros2 = numpy.zeros((mB, mB))
	zeros3 = numpy.zeros((mC, mC))
	row1 = numpy.concatenate((zeros1, A, C.T), axis = 1)
	row2 = numpy.concatenate((A.T, zeros2, B), axis = 1)
	row2 = numpy.concatenate((C, B.T, zeros3), axis = 1)
	P = numpy.concatenate((row1, row2, row3), axis = 0)
	l, v = scipy.linalg.eig(P)
	l = l[::3]
	argsort = numpy.flip(numpy.argsort(l), 0)
	l = l[argsort]
	l = l[:nA]
	v = v[:,::3]
	v = v[:,argsort]
	U = normalize(v[:mA,:nA])
	V = normalize(v[mA:mB,:nA])
	W = normalize(v[mB:,:nA])
	s = l
	return s, U, V, W