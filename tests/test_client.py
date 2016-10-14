import unittest

from articlemeta import client


class ClientTest(unittest.TestCase):

    def test_dates_pagination(self):

        result = [i for i in client.dates_pagination('2016-01-22', '2016-10-01')]

        expected = [
            ('2016-01-22', '2016-02-21'),
            ('2016-02-22', '2016-03-23'),
            ('2016-03-24', '2016-04-23'),
            ('2016-04-24', '2016-05-24'),
            ('2016-05-25', '2016-06-24'),
            ('2016-06-25', '2016-07-25'),
            ('2016-07-26', '2016-08-25'),
            ('2016-08-26', '2016-09-25'),
            ('2016-09-26', '2016-10-01')
        ]

        self.assertEqual(expected, result)
