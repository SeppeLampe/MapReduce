import mrjob.protocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import nltk
import operator
file = open('test.txt', 'w')

WORD_RE = re.compile(r"[\w']+")
class CommonKeywords(MRJob):

    def mapper_get_genre_words(self, _, line):
        line = list(line.split())
        if line[1] == 'movie':           # titleType is on position 1, only select those which are movies
            genres = line[-1].split(',') # Genres are on the last position
            primary_title = nltk.tokenize.word_tokenize(line[2])  # PrimaryTitle is on position 2
            for genre in genres:        # A movie sometimes has multiple genres
                if genre != '\\N':      # We don't want to include movies which don't have a genre specified
                    for word, type in nltk.pos_tag(primary_title):
                        # VBZ = auxiliary verbs, IN = preposition, DT = articles/determinants, CC = conjunction
                        # CNJ = conjunction, PRO = pronoun, TO = the word 'to', P = preposition
                        # We exclude these types along with the symbols (,:.')
                        if type not in ('VBZ', 'IN', 'DT', 'CC', ',', ':', '.', '\'\'' 'CNJ', 'PRO', 'TO', 'P'):
                            yield (genre, word.lower()), 1

    def combiner_count_genre_words(self, genre_words, counts):
        """
        :param genre_words: a tuple/list containing the genre and word of the primary title of a movie
        :param counts: the amount of times this word has been counted in the primary titles of this genre
        :return: (genre, word) as key and the sum of the counts as value
        """
        yield genre_words, sum(counts)

    def reducer_count_genre_words(self, genre_words, counts):
        """
        :param genre_words: a tuple/list containing the genre and word of the primary title of a movie
        :param counts: the amount of times this word has been counted in the primary titles of this genre
        :return: (genre, word) as key and the sum of the counts as value.
        """
        #file.write(str(genre_words[0]) + " " + str(sum(counts)) + " " + str(genre_words[1]) + "\n")
        yield genre_words, sum(counts)

    def mapper_to_genres(self, genre_words, counts):
        """
        :param genre_words: a tuple/list containing the genre and word of the primary title of a movie
        :param counts: the amount of times this word has been counted in the primary titles of this genre
        :return: genre as key, (word, count) as value.
        """
        #file.write(str(list(genre_words)[0]) + " " + str((counts)) + str(list(genre_words)[1]) + "\n")
        yield genre_words[0], (counts, genre_words[1])

    def reducer_find_max_15_word_per_genre(self, genre, word_counts):
        """
        :param genre: the genres
        :param word_counts: tuples/lists containing (word, count) for each of the words in the primary title of this genre
        :return: genre, word
        For each genre, we'll sort the word_counts by its count (position 0) from high to low (reverse=True), then we'll
        select either the first 15 elements or if there are less, we select as many as we can.
        """
        word_counts = list(word_counts)
        amount_of_items = min(len(list(word_counts)), 15)
        for count, word in sorted(word_counts, key=operator.itemgetter(0), reverse=True)[0:amount_of_items]:
            yield genre, word

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_genre_words,
                   combiner=self.combiner_count_genre_words,
                   reducer=self.reducer_count_genre_words),
            MRStep(mapper=self.mapper_to_genres,
                    reducer=self.reducer_find_max_15_word_per_genre)
        ]


if __name__ == '__main__':
    CommonKeywords.run()