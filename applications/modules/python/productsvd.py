#!/usr/bin/env python3

"""

Functions
---------

"""

import numpy
import scipy.linalg
from sklearn.preprocessing import normalize

def qrproductsvd(A, B):
	QA, RA = scipy.linalg.qr(A, mode = "economic")
	QB, RB = scipy.linalg.qr(B, mode = "economic")
	C = RA @ RB.T
	UC, s, VCt = scipy.linalg.svd(C, full_matrices = False)
	U = QA @ UC
	Vt = VCt @ QB.T
	return U, s, Vt

def reduceusv(U, s, Vt, p):
	U = U[:,:p]
	s = s[:p]
	Vt = Vt[:p]
	return U, s, Vt

def svdproductsvd(A, B, p = None, q = None):
	UA, sA, VAt = scipy.linalg.svd(A, full_matrices = False)
	if p is not None:
		UA, sA, VAt = reduceusv(UA, sA, VAt, p)
	UB, sB, VBt = scipy.linalg.svd(B, full_matrices = False)
	if q is not None:
		UB, sB, VBt = reduceusv(UB, sB, VBt, q)
	C = numpy.diag(sA) @ VAt @ VBt.T @ numpy.diag(sB)
	UC, s, VCt = scipy.linalg.svd(C, full_matrices = False)
	U = UA @ UC
	Vt = VCt @ UB.T
	return U, s, Vt

def qrmca(dataSet1, dataSet2, p = None, q = None, r = None):
	dataSet1 = dataSet1 - dataSet1.mean(axis = 0)
	dataSet2 = dataSet2 - dataSet2.mean(axis = 0) # Center before or after PCA?
	if p is None and q is None:
		U, s, Vt = qrproductsvd(dataSet1, dataSet2)
	else:
		U, s, Vt = svdproductsvd(dataSet1, dataSet2, p, q)
	if r is not None:
		U, s, Vt = reduceusv(U, s, Vt, r)
	return U, s, Vt

def reducematrix(A, p):
	U, s, Vt = scipy.linalg.svd(A, full_matrices = False)
	U, s, Vt = reduceusv(U, s, Vt, p)
	A = U @ numpy.diag(s) @ Vt
	return A

def qrcca(dataSet1, dataSet2, p = None, q = None, r = None):
	if p is not None:
		dataSet1 = reducematrix(dataSet1, p)
	if q is not None:
		dataSet2 = reducematrix(dataSet2, q)
	dataSet1 = normalize(dataSet1)
	dataSet2 = normalize(dataSet2)
	U, s, Vt = qrproductsvd(dataSet1, dataSet2)
	if r is not None:
		U, s, Vt = reduceusv(U, s, Vt, r)
	return U, s, Vt