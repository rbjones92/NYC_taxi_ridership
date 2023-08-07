import unittest
from unittest.mock import patch, MagicMock
import pandas as pd

from scrape_weather import get_dates, list_transpose, set_schema

class TestScrapeWeather(unittest.TestCase):

    def test_get_dates(self):
        # Test the get_dates function
        date_list = get_dates()
        self.assertIsInstance(date_list, list)
        self.assertTrue(all(isinstance(d, str) for d in date_list))
        self.assertEqual(date_list[0], "2018-01-01")
        self.assertEqual(date_list[-1], "2018-12-31")

    def test_list_transpose(self):
        # Test the list_transpose function
        input_data = [["10%", "20°F", "30in", "40°", "50mph", "60°F", "70%", "80%", "90%", "100°"]]
        expected_data = [["10", "20", "30", "40", "50", "60", "70", "80", "90", "100"]]
        output_data = list_transpose(input_data)
        self.assertEqual(output_data, expected_data)

    def test_set_schema(self):
        # Test the set_schema function
        df = MagicMock()
        df_return = set_schema(df)
        self.assertEqual(df_return, df)

if __name__ == '__main__':
    unittest.main()
