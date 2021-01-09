from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
from nltk.corpus import stopwords
import operator
import time
import re

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f'{key}: {value[1]}, {value[0]}', 'utf-8')

class TopKeywords(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    def init_mapper(self):
        self.stop_words = [re.sub("'", "\'", word) for language in ('english', 'spanish', 'french', 'german', 'italian')
                           for word in stopwords.words(language)]
        # VBZ = auxiliary verbs, IN = preposition, DT = articles/determinants, CC = conjunction, CNJ = conjunction,
        # PRO = pronoun, POS = possessive ending ('s), P = preposition, the others are the respective symbols.
        self.non_useful_types = ('VBZ', 'IN', 'DT', 'CC', 'CNJ', 'PRO', 'P', 'POS', ',', ':', '.', '\'')

    def mapper_get_genre_words(self, _, line):
        line = list(line.split())
        if line[1] == 'movie':           # titleType is on position 1, only select those which are movies
            genres = line[-1].split(',')  # Genres are on the last position
            primary_title = nltk.pos_tag(nltk.tokenize.word_tokenize(line[2]))  # PrimaryTitle is on position 2
            for genre in genres:        # A movie sometimes has multiple genres
                if genre != '\\N':      # We don't want to include movies which don't have a genre specified
                    for word, type in primary_title:
                        word = word.lower()
                        # Check whether type and word are meaningful
                        if (type not in self.non_useful_types) and (word not in self.stop_words) and word.isalpha():
                            yield (genre, word), 1

    @staticmethod
    def sum_values(genre_word, count):
        # sum the counts
        yield genre_word, sum(count)

    @staticmethod
    def mapper_to_genres(genre_words, counts):
        # yield genre, (count, word)
        yield genre_words[0], (counts, genre_words[1])

    @staticmethod
    def fifteen_per_genre(genre, count_words):
        """
        For each genre, we'll sort the word_counts by its count (position 0) from high to low (reverse=True),
        then we'll select either the first 15 elements or if there are less, we select as many as we can.
        """
        count_words = list(count_words)
        amount_of_items = min(len(count_words), 15)
        for count_words in sorted(count_words, key=operator.itemgetter(0), reverse=True)[0:amount_of_items]:
            yield genre, count_words

    def steps(self):
        return [
            MRStep(mapper_init=self.init_mapper,
                   mapper=self.mapper_get_genre_words,
                   combiner=self.sum_values,
                   reducer=self.sum_values),
            MRStep(mapper=self.mapper_to_genres,
                   combiner=self.fifteen_per_genre,
                   reducer=self.fifteen_per_genre)
        ]


if __name__ == '__main__':
    TopKeywords.run()
