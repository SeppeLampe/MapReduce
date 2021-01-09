from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import nltk
import operator
from sklearn.feature_extraction.text import CountVectorizer
import re

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f"Arxiv id '{value[0]}' has the most similar summary"
                     f" with a cosine similarity of {round(value[1], 3)}", 'utf-8')

class MostSimilarArticle(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    def configure_args(self):
        # This function allows adding extra command line arguments
        super(MostSimilarArticle, self).configure_args()
        # We'll add an argument 'summary_file' which takes in a file containing the summary of a paper
        self.add_file_arg('--source_file', help='Location of txt file which contains the summary of a paper')

    @staticmethod
    def cosine_similarity(vector_a, vector_b):
        # Compute the cosine similarity between two matrices
        dot = sum(a * b for a, b in zip(vector_a, vector_b))
        norm_a = sum(a*a for a in vector_a) ** 0.5
        norm_b = sum(b * b for b in vector_b) ** 0.5
        return dot/(norm_a*norm_b)

    @staticmethod
    def preprocessor(text):
        """
        This function takes one parameter 'text' which is a string and removes newline commands,
        converts the characters to lowercase and filters out everything which is not a word and returns the result
        """
        text = text.replace('\\n', ' ')
        text = text.lower()
        text = re.sub('[^a-zA-Z]+', ' ', text)
        return text

    @staticmethod
    def tokenize_lemmatize(text):
        """
        This function takes one parameter 'text' which is a string and word tokenizes it,
        it then lemmatizes this text and returns the result
        """
        text = nltk.tokenize.word_tokenize(text)
        lemmatizer = nltk.stem.wordnet.WordNetLemmatizer()
        text = [lemmatizer.lemmatize(word) for word in text]
        return text

    def get_id_summary(self, _, uri):
        # Read the text we're comparing to into a string variable
        with open(self.options.source_file, "r") as source_file:
            source = source_file.read()
        # Read in the actual data
        with open(uri, 'r') as input_file:
            data = json.load(input_file)  # Will be a of dictionaries
        # For each object (info about one specific paper)
        for paper in data:
            # vectorize the summary and source file
            cv = CountVectorizer((source, paper['summary']), preprocessor=self.preprocessor,
                                 stop_words='english', tokenizer=self.tokenize_lemmatize)
            vector1, vector2 = cv.fit_transform((source, paper['summary'])).toarray()
            # Get the cosine similarity between the two files
            cosine_similarity = self.cosine_similarity(vector1, vector2)
            # yield no key and the (paper_id, cosine_similarity) as value
            yield None, (paper['id'], cosine_similarity)

    @staticmethod
    def max_value(_, id_cosine_similarity):
        # Get the tuple with the maximum cosine_similarity
        id_cosine_similarity = tuple(id_cosine_similarity)
        # cosine_similarity is on index 1
        most_similar = max(id_cosine_similarity, key=operator.itemgetter(1))
        yield None, most_similar

    def steps(self):
        return [
            MRStep(mapper_raw=self.get_id_summary,
                   combiner=self.max_value,
                   reducer=self.max_value)
        ]


if __name__ == '__main__':
    MostSimilarArticle.run()

# python Task_5.py -r local -v --no-bootstrap-mrjob --source_file summary.txt <"arxivDatasmall.json" > Task_5.txt