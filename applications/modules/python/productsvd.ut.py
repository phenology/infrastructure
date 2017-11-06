import unittest
import productsvd
import numpy

class TestReduceusv(unittest.TestCase):
	def test_1(self):
		Ui = numpy.array([[1,0,0],[0,1,0],[0,0,1],[0,0,0]])
		si = numpy.array([3,2,1])
		Vit = numpy.array([[1,0,0],[0,1,0],[0,0,1]])
		p = 2
		Ue = numpy.array([[1,0],[0,1],[0,0],[0,0]])
		se = numpy.array([3,2])
		Vet = numpy.array([[1,0,0],[0,1,0]])
		Uo, so, Vot = productsvd.reduceusv(Ui, si, Vit, p)
		numpy.testing.assert_array_equal(Ue, Uo)
		numpy.testing.assert_array_equal(se, so)
		numpy.testing.assert_array_equal(Vet, Vot)

class TestReducematrix(unittest.TestCase):
	def test_1(self):
		Ui = numpy.array([[1,0,0],[0,1,0],[0,0,1],[0,0,0]])
		si = numpy.array([3,2,1])
		Vit = numpy.array([[1,0,0],[0,1,0],[0,0,1]])
		p = 2
		Ai = Ui @ numpy.diag(si) @ Vit
		Ue = numpy.array([[1,0],[0,1],[0,0],[0,0]])
		se = numpy.array([3,2])
		Vet = numpy.array([[1,0,0],[0,1,0]])
		Ae = Ue @ numpy.diag(se) @ Vet
		Ao = productsvd.reducematrix(Ai, p)
		numpy.testing.assert_array_equal(Ae, Ao)

class TestSvdproductsvd(unittest.TestCase):
	def test_1(self):
		Ai = numpy.random.randint(0, 9, size = (9,5))
		Bi = numpy.random.randint(0, 9, size = (9,5))
		Ci = Ai @ Bi.T
		Ue, se, Vet = numpy.linalg.svd(Ci, full_matrices = False)
		Ce = Ci
		Ue, se, Vet = productsvd.reduceusv(Ue, se, Vet, 5)
		Ue = numpy.absolute(Ue)
		Vet = numpy.absolute(Vet)
		Uo, so, Vot = productsvd.svdproductsvd(Ai, Bi)
		Co = Uo @ numpy.diag(so) @ Vot
		Uo = numpy.absolute(Uo)
		Vot = numpy.absolute(Vot)
		numpy.testing.assert_array_almost_equal(Ue, Uo)
		numpy.testing.assert_array_almost_equal(se, so)
		numpy.testing.assert_array_almost_equal(Vet, Vot)
		numpy.testing.assert_array_almost_equal(Ce, Co)

class TestQrproductsvd(unittest.TestCase):
	def test_1(self):
		Ai = numpy.random.randint(0, 9, size = (9,5))
		Bi = numpy.random.randint(0, 9, size = (9,5))
		Ci = Ai @ Bi.T
		Ue, se, Vet = numpy.linalg.svd(Ci, full_matrices = False)
		Ce = Ci
		Ue, se, Vet = productsvd.reduceusv(Ue, se, Vet, 5)
		Ue = numpy.absolute(Ue)
		Vet = numpy.absolute(Vet)
		Uo, so, Vot = productsvd.qrproductsvd(Ai, Bi)
		Co = Uo @ numpy.diag(so) @ Vot
		Uo = numpy.absolute(Uo)
		Vot = numpy.absolute(Vot)
		numpy.testing.assert_array_almost_equal(Ue, Uo)
		numpy.testing.assert_array_almost_equal(se, so)
		numpy.testing.assert_array_almost_equal(Vet, Vot)
		numpy.testing.assert_array_almost_equal(Ce, Co)

if __name__ == '__main__':
	unittest.main()