import unittest
from manager.string_preprocess import SpellingReplacer
from manager.string_preprocess import word_correction
from manager.string_preprocess import tokenized_word


class TestSpellingReplacer(unittest.TestCase):
    def test_correct_value(self):
        input_value = 'SPELLING'
        expected_value = 'SPELLING'
        replacer = SpellingReplacer()
        res = replacer.replace(input_value)
        self.assertEqual(res, expected_value)

    def test_incorrect_value(self):
        input_value = 'SPELLIN'
        expected_value = 'SPELLING'
        replacer = SpellingReplacer()
        res = replacer.replace(input_value)
        self.assertEqual(res, expected_value)


class TestWordCorrection(unittest.TestCase):
    def test_correct_addrv_value(self):
        abbrv = {
            'AVE': 'Avenue'
        }
        input_value = ['AVE']
        expected_value = ['AVENUE']
        res = word_correction(input_value, abbrv)
        self.assertEqual(res, expected_value)

    def test_correct_value(self):
        abbrv = {
            'AVE': 'Avenue'
        }
        input_value = ['SPELLIN']
        expected_value = ['SPELLING']
        res = word_correction(input_value, abbrv)
        self.assertEqual(res, expected_value)


class TestTokenizedWord(unittest.TestCase):
    def test_valid(self):
        input_value = "Hello, world!"
        expected_value = ['Hello', 'world']
        res = tokenized_word(input_value)
        self.assertEqual(res, expected_value)

    def test_not_valid(self):
        input_value = "Hello, world!"
        expected_value = ['Hello', ',', 'world', '!']
        res = tokenized_word(input_value)
        self.assertNotEqual(res, expected_value)