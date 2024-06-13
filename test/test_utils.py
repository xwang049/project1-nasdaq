import unittest
from src.utils import download_all_tables

class TestUtils(unittest.TestCase):
    def test_download_all_tables(self):
        tables_filters = {
            'QDL/FON': {}
        }
        download_all_tables(tables_filters)

if __name__ == '__main__':
    unittest.main()
