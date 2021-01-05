from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import operator
import time

class CommonKeywords(MRJob):

    def mapper_get_year_customer_price(self, _, line):
        line = line.split(',')
        try:
            # The csv (comma-separated value) file has a field (description) which can contain commas...
            # This field is thus split into two fields by the split method called above.
            # Indexing has therefore been done from the back to avoid ValueErrors when selecting the price/quantity/date
            stockcode, quantity, price = line[1], float(line[-5]), float(line[-3])
            year = datetime.datetime.strptime(line[-4], '%m/%d/%Y %H:%M:%S').year
        except ValueError:
            # This ensures that the file header does not throw an error and stops the execution
            pass
        else:
            # Get rid of customer fields which are blank as these will all be added to one customer: ""
            if stockcode:
                cost = quantity*price
                yield (year, stockcode), (quantity, cost)

    def combiner_sum_bill(self, year_stock, stats):
        stats_list = list(stats)
        quantity = sum([row[0] for row in stats_list])
        price = sum([row[1] for row in stats_list])
        yield year_stock, (quantity, price)

    def reducer_sum_bill(self, year_stock, stats):
        stats_list = list(stats)
        quantity = sum([row[0] for row in stats_list])
        price = sum([row[1] for row in stats_list])
        yield year_stock, (quantity, price)

    def mapper_to_years(self, year_stock, yearly_stats):
        yield year_stock[0], (yearly_stats[0], yearly_stats[1], year_stock[1]) # Year, (Quantity, price, stockcode)

    def reducer_find_max_10_per_year(self, year, yearly_stock_stats):
        yearly_stock_stats = list(yearly_stock_stats)
        highest_quantity = sorted(yearly_stock_stats, key=operator.itemgetter(0), reverse=True)[0]
        highest_price = sorted(yearly_stock_stats, key=operator.itemgetter(1), reverse=True)[0]
        yield year, f"The item that was sold the most was stockcode '{highest_quantity[2]}' of which {highest_quantity[0]} items were sold"
        yield year, f"The item that was sold for the most money was stockcode '{highest_price[2]}', which was sold for {highest_price[1]}$"

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_year_customer_price,
                   #combiner=self.combiner_sum_bill,
                   reducer=self.reducer_sum_bill),
            MRStep(mapper=self.mapper_to_years,
                   reducer=self.reducer_find_max_10_per_year)
        ]

if __name__ == '__main__':
    CommonKeywords.run()