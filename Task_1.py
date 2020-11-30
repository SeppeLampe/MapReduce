from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import nltk

WORD_RE = re.compile(r"[\w']+")
class CommonKeywords(MRJob):

    def mapper_get_words(self, _, line):
        line = list(line.split())
        if line[1] in ('movie', 'short'):           # titleType is on position 1
            primary_title = nltk.tokenize.word_tokenize(line[2])  # PrimaryTitle is on position 2
            for word, type in nltk.pos_tag(primary_title):
                # VBZ = auxiliary verbs (actually third person singular present), IN = preposition, DET = articles/determinants, CC = conjunction
                if type not in ('IN', 'DT', 'CC', 'VBZ', ',', ':', '.', '\'', 'CNJ', 'PRP', 'TO'):
                    yield word.lower(), 1

    def combiner_count_words(self, word, counts):
        '''
        this combiner sums the words we've seen so far
        :param word: word obtained from the mapper
        :param counts: 1
        :return: (word, sum)
        '''
        yield word, sum(counts)

    def reducer_count_words(self, word, counts):
        '''
        this reducer sends all (num_occurrences, word) pairs to the same final reducer.
        num_occurrences is so we can easily use Python's max() function.
        :param word: word obtained from the combiner
        :param counts: the number of occurrences of the word from the result of the combiner
        :return: (None, (sum(counts), word))
        '''
        yield None, (sum(counts), word)

    def reducer_find_max_50_word(self, _, word_count_pairs):
        '''
        this final reducer gets the most commonly used word
        :param _: discard the key; it is just None
        :param word_count_pairs: each item of word_count_pairs is (count, word),
        :return: (key=counts, value=word)
        '''
        for key in sorted(word_count_pairs, reverse=True)[:50]:
            yield key

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_50_word)
        ]



if __name__ == '__main__':
    CommonKeywords.run()