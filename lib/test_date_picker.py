import unittest

from datetime import timedelta, datetime
from lib.date_picker import missing_dates, dates_between, create_datetime

class TestDagStarter(unittest.TestCase):
    def setUp(self):
        self.start_date = create_datetime("2023-02-01")
        self.end_date = create_datetime("2023-02-23")

    def test_empty_input(self):
        dag_dates = missing_dates(self.start_date, self.end_date, [])
        self.assertEqual(len(dag_dates), 22)
    
    def test_one_existing_date(self):
        test_date = create_datetime("2023-02-02")
        dag_dates = missing_dates(self.start_date, self.end_date, [test_date])
        self.assertEqual(len(dag_dates), 21)
        self.assertFalse(test_date in dag_dates)

    def test_all_dates_exist(self):
        test_dates = dates_between(self.start_date, self.end_date)
        dag_dates = missing_dates(self.start_date, self.end_date, test_dates)
        self.assertEqual(len(dag_dates), 0)

if __name__ == '__main__':
    unittest.main()