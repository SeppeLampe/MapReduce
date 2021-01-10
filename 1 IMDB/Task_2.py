# To run inline:
# python "1 IMDB\Task_2.py" "1 IMDB\title.basics.tsv" > "1 IMDB\Task_2.txt"

# To run on a local cluster:
# python "1 IMDB\Task_2.py" -r local --no-bootstrap-mrjob "1 IMDB\title.basics.tsv" > "1 IMDB\Task_2.txt"

from mrjob.job import MRJob
from mrjob.step import MRStep
import nltk
from nltk.corpus import stopwords
import operator
import re

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f'{key}: {value[1]}, {value[0]}', 'utf-8')

class TopKeywords(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    def init_mapper(self):
        # We'll generate a list with words to avoid: common stopwords in the most common languages
        self.stop_words = [re.sub("'", "\'", word) for language in ('english', 'spanish', 'french', 'german', 'italian')
                           for word in stopwords.words(language)]
        # We'll also ignore the word 'untitled'
        self.stop_words.append('untitled')
        # VBZ = auxiliary verbs, IN = preposition, DT = articles/determinants, CC = conjunction, CNJ = conjunction,
        # PRO = pronoun, POS = possessive ending ('s), P = preposition, the others are the respective symbols.
        self.non_useful_types = ('VBZ', 'IN', 'DT', 'CC', 'CNJ', 'PRO', 'P', 'POS', ',', ':', '.', '\'')

    def mapper_get_genre_words(self, _, line):
        line = list(line.split())
        if line[1] == 'movie':           # titleType is on position 1, only select those which are movies
            primary_title = nltk.pos_tag(nltk.tokenize.word_tokenize(line[2]))  # PrimaryTitle is on position 2
            genres = line[-1].split(',')  # Genres are on the last position
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
    def mapper_to_genres(genre_words, count):
        # yield genre, (count, word)
        yield genre_words[0], (count, genre_words[1])

    @staticmethod
    def fifteen_per_genre(genre, count_words):
        """
        Mrjob sorts the pairs based on 'word', no matter the order in which count_words or word_counts is defined.
        So we need to sort them ourselves based on count and then select the 15 highest for each genre
        """
        count_words = list(count_words)
        for count_words in sorted(count_words, key=operator.itemgetter(0), reverse=True)[:15]:
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
