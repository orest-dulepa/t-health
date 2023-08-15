import unittest
from mpfs_pricer import utils


class DatesTestCase(unittest.TestCase):
    def test_get_quarter(self):
        quarter = utils.get_quarter('8/8/2020')
        self.assertEqual(quarter, '2020_Q3')

        quarter = utils.get_quarter('9/17/2020')
        self.assertEqual(quarter, '2020_Q3')

        quarter = utils.get_quarter('11/9/2019')
        self.assertEqual(quarter, '2019_Q4')

        quarter = utils.get_quarter('12/7/2019')
        self.assertEqual(quarter, '2019_Q4')

        quarter = utils.get_quarter('12/8/2019')
        self.assertEqual(quarter, '2019_Q4')

        quarter = utils.get_quarter('2/20/2020')
        self.assertEqual(quarter, '2020_Q1')

        quarter = utils.get_quarter('5/31/2018')
        self.assertEqual(quarter, '2018_Q2')

        quarter = utils.get_quarter('1/14/2021')
        self.assertEqual(quarter, '2021_Q1')

        quarter = utils.get_quarter('12/16/2019')
        self.assertEqual(quarter, '2019_Q4')

        quarter = utils.get_quarter('3/1/2020')
        self.assertEqual(quarter, '2020_Q1')

        quarter = utils.get_quarter('2/8/2021')
        self.assertEqual(quarter, '2021_Q1')

        quarter = utils.get_quarter('2/10/2021')
        self.assertEqual(quarter, '2021_Q1')


if __name__ == '__main__':
    unittest.main()
