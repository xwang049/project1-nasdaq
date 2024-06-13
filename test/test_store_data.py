import unittest
import pandas as pd
from src.store_data import save_to_db, engine

class TestStoreData(unittest.TestCase):
    def test_save_to_db(self):
        data = {'column1': [1, 2, 3], 'column2': ['a', 'b', 'c']}
        dataframe = pd.DataFrame(data)
        table_name = 'test_table'
        save_to_db(dataframe, table_name, engine)

if __name__ == '__main__':
    unittest.main()
