import unittest
from unittest.mock import patch, Mock, call
import os
from scrape_nyc_data import ScrapeNyTaxi

class TestScrapeNyTaxi(unittest.TestCase):
    @patch('requests.get')
    @patch('builtins.open', new_callable=unittest.mock.mock_open, create=True)
    @patch('os.makedirs')
    def test_grab_data(self, mock_makedirs, mock_open, mock_requests_get):
        # Mock the response from requests.get
        mock_response = Mock()
        mock_response.content = b'Test Content'
        mock_requests_get.return_value = mock_response
        # TEST DATES
        date = '2022-01'
        # TEST DIRECTORY
        directory = 'yellow'
        ScrapeNyTaxi.grab_data(date=date, directory=directory)
        # Define the expected file path based on the inputs
        expected_file_path = os.path.join(directory, f'{directory}_{date}.parquet')
        # Assert that the necessary functions and methods were called with the correct arguments
        mock_requests_get.assert_called_once_with(
            f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet',
            allow_redirects=True
        )
        mock_makedirs.assert_called_once_with(directory, exist_ok=True)
        mock_open.assert_called_once_with(expected_file_path, 'wb')
        mock_open().write.assert_called_once_with(b'Test Content')

    @patch('scrape_nyc_data.ScrapeNyTaxi.grab_data')
    def test_get_all_data(self, mock_grab_data):
        ScrapeNyTaxi.get_all_data()
        expected_calls = [call(date=date, directory='yellow') for date in ScrapeNyTaxi.yellow_dates]
        mock_grab_data.assert_has_calls(expected_calls)

if __name__ == '__main__':
    unittest.main()
