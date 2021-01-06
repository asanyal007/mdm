import unittest
import pandas as pd
from manager.text_mdm_ratio import mdm_text


class TestAddressMDM(unittest.TestCase):
    def test_valid(self):
        abbrv = {
            'FYI': 'For your info'
        }
        df = pd.DataFrame([{'col': ['HELLO', 'WORLD']},
                           {'col': ['HELLO', 'WORLD', 'FYI']}])

        expected_df = pd.DataFrame([{'col': 'HELLO WORLD FOR YOUR INFO'},
                                    {'col': 'HELLO WORLD FOR YOUR INFO'}])
        df['col'] = ' '.join(mdm_text(df, 'col', abbrv))

        self.assertTrue(df.equals(expected_df))

    def test_not_valid(self):
        abbrv = {
            'FYI': 'For your info'
        }
        df = pd.DataFrame([{'col': ['HELLO', 'WORLD', 'NO#001']},
                           {'col': ['HELLO', 'WORLD', 'FYI', 'NO', '001']}])

        expected_df = pd.DataFrame([{'col': 'HELLO WORLD FOR YOUR INFO NO NO#001'},
                                    {'col': 'HELLO WORLD FOR YOUR INFO NO NO#001'}])
        df['col'] = ' '.join(mdm_text(df, 'col', abbrv))

        self.assertFalse(df.equals(expected_df))
