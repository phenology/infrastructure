import unittest
import generalisedproductsvd
import numpy

class TestSvd2(unittest.TestCase):
	def test_1(self):
		Ui = numpy.array([[1,0,0],[0,1,0],[0,0,1],[0,0,0]])
		si = numpy.array([3,2,1])
		Vit = numpy.array([[1,0,0],[0,1,0],[0,0,1]])
		Ai = Ui @ numpy.diag(si) @ Vit
		Ue = numpy.array([[1,0,0],[0,1,0],[0,0,1],[0,0,0]])
		se = numpy.array([3,2,1])
		Vet = numpy.array([[1,0,0],[0,1,0],[0,0,1]])
		Ue = numpy.absolute(Ue)
		Vet = numpy.absolute(Vet)
		Uo, so, Vot = generalisedproductsvd.svd2(Ai)
		Uo = numpy.absolute(Uo)
		Vot = numpy.absolute(Vot)
		numpy.testing.assert_array_almost_equal(Ue, Uo)
		numpy.testing.assert_array_almost_equal(se, so)
		numpy.testing.assert_array_almost_equal(Vet, Vot)

if __name__ == '__main__':
	unittest.main()