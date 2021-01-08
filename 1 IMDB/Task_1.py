from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
from nltk.corpus import stopwords
import time
import re

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, _, count_word):
        return bytes(f'{count_word[1]}: {count_word[0]}', 'utf-8')

class CommonKeywords(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol
    def init_mapper(self):
        self.stop_words = [re.sub("'", "\'", word) for language in ('english', 'spanish', 'french', 'german', 'italian')
                           for word in stopwords.words(language)]
        # VBZ = auxiliary verbs, IN = preposition, DT = articles/determinants, CC = conjunction, CNJ = conjunction,
        # PRO = pronoun, POS = possessive ending ('s), P = preposition, the others are the respective symbols.
        self.non_useful_types = ('VBZ', 'IN', 'DT', 'CC', 'CNJ', 'PRO', 'P', 'POS', ',', ':', '.', '\'')

    def mapper_get_words(self, _, line):
        line = list(line.split())
        if line[1] in ('movie', 'short'):  # titleType is on position 1
            primary_title = nltk.tokenize.word_tokenize(line[2])  # PrimaryTitle is on position 2
            for word, type in nltk.pos_tag(primary_title):
                word = word.lower()
                # Check whether type and word are meaningful
                if (type not in self.non_useful_types) and (word not in self.stop_words) and word.isalpha():
                    yield word, 1

    @staticmethod
    def sum_values(word, count):
        # return word as key, sum of counts as value
        yield word, sum(count)

    @staticmethod
    def mapper_None_count_word(word, counts):
        # For each key word yield no key and (count, word) as value
        yield None, (counts, word)

    @staticmethod
    def fifty_max_values(_, count_words):
        # Sort the count, word pairs based on count and select the 50 highest
        for count_word in sorted(count_words, reverse=True)[:50]:
            yield None, count_word

    def steps(self):
        return [
            MRStep(mapper_init=self.init_mapper,
                   mapper=self.mapper_get_words,
                   combiner=self.sum_values,
                   reducer=self.sum_values),
            MRStep(mapper=self.mapper_None_count_word,
                   combiner=self.fifty_max_values,
                   reducer=self.fifty_max_values)
        ]


if __name__ == '__main__':
    start = time.time()
    CommonKeywords.run()
    total_time = int(time.time() - start)
    print(f'Took {total_time//60} minutes and {total_time%60} seconds to compute')
