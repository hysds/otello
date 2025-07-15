import unittest
from unittest.mock import patch, MagicMock
from requests.auth import HTTPBasicAuth
from otello.auth import get_authentication_token
import unittest


class TestAuth(unittest.TestCase):
    @patch('getpass.getpass')
    @patch('requests.get')
    def test_get_authentication_token(self, mock_requests_get, mock_getpass):
        # Mock the password input
        mock_getpass.return_value = 'test_password'

        # Mock the requests.get call
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {'result': 'test_token'}
        mock_requests_get.return_value = mock_response

        # Call the function
        token = get_authentication_token('testhost', 'testuser')

        # Assert that the token is correct
        self.assertEqual(token, 'test_token')

        # Assert that getpass was called
        mock_getpass.assert_called_once()

        # Assert that requests.get was called with the correct arguments
        mock_requests_get.assert_called_once_with(
            'https://testhost/mozart/api/v0.1/login',
            auth=HTTPBasicAuth('testuser', 'test_password')
        )


if __name__ == '__main__':
    unittest.main()
