import unittest

from articlemeta import client


class ClientTest(unittest.TestCase):

    def test_dates_pagination(self):

        result = [i for i in client.dates_pagination('2013-01-22', '2016-10-01')]

        expected = [
            ('2013-01-22', '2013-12-31'),
            ('2014-01-01', '2014-12-31'),
            ('2015-01-01', '2015-12-31'),
            ('2016-01-01', '2016-10-01')
        ]

        self.assertEqual(sorted(expected), sorted(result))
