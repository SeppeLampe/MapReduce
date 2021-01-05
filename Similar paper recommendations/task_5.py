from mrjob.job import MRJob
from mrjob.step import MRStep
import time
from sklearn.feature_extraction.text import CountVectorizer


class MRMostSimilarArticle(MRJob):
    source = """Recent approaches based on ANNs have shown pretty promising results for text classification.
    However, many short texts occur in sequences , and most existing ANN-based systems do not leverage the
    preceding texts when classifying a subsequent one. We present a model based on recurrent
    neural networks and convolutional neural networks that incorporates the preceding texts. Our model
    achieves state-of-the-art results on 3 different data sets for dialog act prediction."""
    # source text for which we want to find the most similar article


    def cosine_similarity(self, vector_a, vector_b):
        """
        function for calculating cosine similarity between 2 vectors
        :param vector_a: first vector
        :param vector_b: second vector
        :return: cosine similarity [0;1]
        """
        dot = sum(a * b for a, b in zip(vector_a, vector_b))
        norm_a = sum(a*a for a in vector_a) ** 0.5
        norm_b = sum(b * b for b in vector_b) ** 0.5
        return dot/(norm_a*norm_b)

    def mapper_get_similarity(self, _, line):
        """
        this mapper yields every article summary with its cosine similarity score corresponding to the source
        :param _: None
        :param line: one line from the input file, which is an article summary
        :return: (None, (line, cosine)
        """
        both_texts = [self.source, line]
        both_texts_to_vector = CountVectorizer().fit_transform(both_texts)
        text_1_to_vector = both_texts_to_vector.toarray()[0].tolist()
        text_2_to_vector = both_texts_to_vector.toarray()[1].tolist()
        cosine = self.cosine_similarity(text_1_to_vector, text_2_to_vector)
        yield None, (line, cosine)

    def combiner_get_batch_most_similar(self, _, papers):  # is it reasonable to use?
        """
        this combiner finds the most similar article from the batch locally
        :param _: None
        :param papers: (line, cosine)
        :return: (None, (article, max_batch_similarity)
        """
        max_batch_similarity = 0
        article = ''
        for paper in papers:
            if max_batch_similarity < paper[1]:   # add  != 1 to ignore same article
                max_batch_similarity = paper[1]
                article = paper[0]

        yield None, (article, max_batch_similarity)

    def reducer_find_most_similar(self, _, papers):
        """
        this final reducer finds the most similar article to the source
        :param _: None
        :param papers: (line, cosine)
        :return: article, max_batch_similarity
        """
        max_similarity = 0
        article = ''
        for paper in papers:
            if max_similarity < paper[1]:
                max_similarity = paper[1]
                article = paper[0]

        yield article, max_similarity

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_similarity,
                   combiner=self.combiner_get_batch_most_similar,
                   reducer=self.reducer_find_most_similar)
        ]


if __name__ == '__main__':
    start = time.time()
    MRMostSimilarArticle.run()  # MRSimilarArticle is job class
    print("Finding most similar paper took {} seconds".format(time.time() - start))

# python task_5.py arxivData.txt