from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import operator
import time

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f'Customer {value[1]} spent \u20ac{round(value[0], 2)} in {key}', 'utf-8')

class YearlyTopCustomers(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    @staticmethod
    def mapper_year_customer_with_cost(_, line):
        line = line.split(',')
        if line[0] != 'Invoice':  # This ensures that the file header is not included
            """
            The csv (comma-separated value) file has a field (description) which can contain commas...
            This field is thus split into two fields by the split method called above.
            Indexing has therefore been done from the back to avoid ValueErrors when selecting the price/amount/date
            """
            customer, quantity, price = line[-2], float(line[-5]), float(line[-3])
            year = datetime.datetime.strptime(line[-4], '%m/%d/%Y %H:%M:%S').year

            # Get rid of customer fields which are blank as these will all be added to one customer: ""
            if customer:
                yield (year, customer), quantity*price

    @staticmethod
    def sum_value(year_customer, cost):
        # Sum the values
        yield year_customer, sum(cost)

    @staticmethod
    def mapper_to_years(year_customer, yearly_cost):
        # yield year, (cost, customer)
        yield year_customer[0], (yearly_cost, year_customer[1])

    @staticmethod
    def max_10_per_year(year, cost_customers):
        """
        The cost_customer will be sorted (high to low due to reverse=True) based on the first (0th) value
        of each sublist. Then only the first 10 sublists (the ones with the highest price) are selected and yielded.
        """
        cost_customers = list(cost_customers)
        for cost_customer in sorted(cost_customers, key=operator.itemgetter(0), reverse=True)[0:10]:
            yield year, cost_customer

    def steps(self):
        return [
            MRStep(mapper=self.mapper_year_customer_with_cost,
                   combiner=self.sum_value,
                   reducer=self.sum_value),
            MRStep(mapper=self.mapper_to_years,
                   combiner=self.max_10_per_year,
                   reducer=self.max_10_per_year)
        ]


if __name__ == '__main__':
    start = time.time()
    YearlyTopCustomers.run()
    total_time = int(time.time() - start)
    print(f'Took {total_time//60} minutes and {total_time%60} seconds to finish')
