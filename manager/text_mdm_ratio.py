"""Group the similarity string and calculate similarity ratio"""
from nltk.metrics import edit_distance


def mdm_text(df, column, addrv):
    """
    text mdm mapping with original by different address sentences
    """
    res = []
    for row in df[column]:
        for word in row.split(' '):
            if word in addrv.keys():
                word = str(addrv[word]).upper()
            if word not in res:
                res.append(word)
    from nltk.stem import PorterStemmer
    ps = PorterStemmer()
    stemmer_list = res
    for word in res:
        root_word = str(ps.stem(word)).upper()
        if root_word in stemmer_list and word != root_word:
            stemmer_list.remove(root_word)
    return stemmer_list


def levenshtein_ratio(src, dist):
    """
    calculate ratio using levenshtein formula
    Arg:
        src: String one input
        dist: string two input
    Return:
        lev_ratio : percentage of string matched
    """
    if len(src) > 0 or len(dist) > 0:
        lev_dist = edit_distance(src, dist)
        lev_ratio = ((len(src) + len(dist)) - lev_dist) / (len(src) + len(dist)) * 100

        return round(lev_ratio, 4)
    else:
        return 100

