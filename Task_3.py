from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import datetime
import operator
import nltk
file = open('test.txt', 'w', encoding='utf-8')

WORD_RE = re.compile(r"[\w']+")
class CommonKeywords(MRJob):

    def mapper_get_year_customer_price(self, _, line):
        line = line.split(',')
        try:
            # The csv (comma-separated value) file has a field (description) which can contain commas...
            # This field is thus split into two fields by the split method called above.
            # Indexing has therefore been done from the back to avoid ValueErrors when selecting the price/amount/date
            customer, quantity, price = line[-2], float(line[-5]), float(line[-3])
            year = datetime.datetime.strptime(line[-4], '%m/%d/%Y %H:%M:%S').year
        except ValueError:
            # This ensures that the file header does not throw an error and stops the execution
            pass
        else:
            # Get rid of customer fields which are blank as these will all be added to one customer: ""
            if customer:
                yield (year, customer), quantity*price

    def combiner_sum_bill(self, year_customer, bill):
        """
        :param year_customer: a tuple/list containing the year and customerID
        :param bill: the amount (quantity*price) a customer spent on an order
        :return: the same key but all the orders summed to get a total bill
        """
        yield year_customer, sum(bill)

    def reducer_sum_bill(self, year_customer, bill):
        """
        :param year_customer: a tuple/list containing the year and customerID
        :param bill: the amount (quantity*price) a customer spent on an order
        :return: the same key but all the orders summed to get a total bill
        """
        yield year_customer, sum(bill)

    def mapper_to_years(self, year_customer, yearly_bill):
        """
        :param year_customer: a tuple/list containing the year and customerID
        :param yearly_bill: the sum of the amount (quantity*price) a customer spent on an order during a specific year
        :return: the year as key, (yearly_bill, customerID) as value. In the next step we'll sort for each year (key)
        its values (yearly_bill, customerID) by yearly_bill and select the matching customerIDs.
        """
        yield year_customer[0], (yearly_bill, year_customer[1])

    def reducer_find_max_10_per_year(self, year, bill_customer):
        """
        :param year: the year for which the data is relevant
        :param bill_customer: a list which contains the total amount spent (that year) and customerID
        :return: year, customerID
        The bill_customer will be sorted (high to low due to reverse=True) based on the first (0th) value
        of each sublist. Then only the first 10 sublists (the ones with the highest price) are selected and yielded.
        """
        bill_customer = list(bill_customer)
        for yearly_bill, customer in sorted(bill_customer, key=operator.itemgetter(0), reverse=True)[0:10]:
            yield year, customer

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_year_customer_price,
                   combiner=self.combiner_sum_bill,
                   reducer=self.reducer_sum_bill),
            MRStep(mapper=self.mapper_to_years,
                   reducer=self.reducer_find_max_10_per_year)
        ]



if __name__ == '__main__':
    CommonKeywords.run()