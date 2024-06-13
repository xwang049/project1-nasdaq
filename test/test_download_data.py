import unittest
from src.download_data import download_data

class TestDownloadData(unittest.TestCase):
    def test_download_data(self):
        table = 'QDL/FON'
        filters = {}
        table_name, data = download_data(table, **filters)
        self.assertIsNotNone(data)
        self.assertEqual(table_name, table)

if __name__ == '__main__':
    unittest.main()
