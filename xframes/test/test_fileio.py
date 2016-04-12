import unittest

from xframes import fileio


class TestFileioLength(unittest.TestCase):
    """
    Tests length function
    """

    def test_length_file(self):
        path = 'files/test-frame-auto.csv'
        length = fileio.length(path)
        self.assertEqual(166, length)

    def test_length_fdir(self):
        path = 'files/test-frame'
        length = fileio.length(path)
        self.assertEqual(575, length)


if __name__ == '__main__':
    unittest.main()
