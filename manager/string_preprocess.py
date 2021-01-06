""" Text tokenizing and cleanup """
import enchant
from nltk.metrics import edit_distance
from nltk.tokenize import word_tokenize


class SpellingReplacer:
    """
    Spelling Replacer with best Suggestions
    """
    def __init__(self, dict_name='en_US', max_dist=2):
        self.spell_dict = enchant.Dict(dict_name)
        self.max_dist = max_dist

    def replace(self, word):
        if self.spell_dict.check(word):
            return word
        suggestions = self.spell_dict.suggest(word)

        if suggestions and edit_distance(word, suggestions[0]) <= self.max_dist:
            return suggestions[0]
        else:
            return word


def word_correction(word_list, abbrv):
    """word correction via spelling replace with SpellingReplacer / data dictionary
    Args:
        word_list : tokenize list sentence
        abbrv : string abbreviation data dictionary
    """
    checked_list = []
    for item in word_list:
        if item in abbrv.keys():
            r = str(abbrv[item]).upper()
        else:
            replacer = SpellingReplacer()
            r = replacer.replace(item)
        checked_list.append(r)
    return checked_list


def tokenized_word(text):
    """
    Remove special characters and tokenize the text to words
    text: row text
    return: list of tokenized words
    """
    if len(text) > 0:
        bad_char = ",.!@#$;:!*%)(&^~-"
        for char in bad_char:
            text = text.replace(char, " ").strip()
        return word_tokenize(text)
    return word_tokenize(text)



