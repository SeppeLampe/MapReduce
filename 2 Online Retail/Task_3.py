# To run inline:
# python "2 Online Retail\Task_3.py" "2 Online Retail\retail0910.csv" "2 Online Retail\retail1011.csv" > "2 Online Retail\Task_3.txt"

# To run on a local cluster:
# python "2 Online Retail\Task_3.py" -r local --no-bootstrap-mrjob "2 Online Retail\retail0910.csv" "2 Online Retail\retail1011.csv" > "2 Online Retail\Task_3.txt"

from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import operator

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def write(self, key, value):
        return bytes(f'Customer {value[1]} spent \u20ac{round(value[0], 2)} in {key}', 'utf-8')

class YearlyTopCustomers(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    @staticmethod
    def mapper_year_customer_with_revenue(_, line):
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
    def sum_value(year_customer, revenue):
        # Sum the values
        yield year_customer, sum(revenue)

    @staticmethod
    def mapper_to_years(year_customer, yearly_revenue):
        # yield year, (revenue, customer)
        yield year_customer[0], (float(yearly_revenue), year_customer[1])

    @staticmethod
    def max_10_per_year(year, revenue_customers):
        """
        Mrjob sorts the pairs based on 'customer', no matter the order in which revenue_customers or customer_revenues
        is defined. So we need to sort them ourselves based on revenue and then select the 10 highest for each year
        """
        revenue_customers = list(revenue_customers)
        for revenue_customer in sorted(revenue_customers, key=operator.itemgetter(0), reverse=True)[:10]:
            yield year, revenue_customer

    def steps(self):
        return [
            MRStep(mapper=self.mapper_year_customer_with_revenue,
                   combiner=self.sum_value,
                   reducer=self.sum_value),
            MRStep(mapper=self.mapper_to_years,
                   combiner=self.max_10_per_year,
                   reducer=self.max_10_per_year)
        ]


if __name__ == '__main__':
    YearlyTopCustomers.run()
