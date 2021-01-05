from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import operator
import time

class MostPopularItems(MRJob):
    def mapper_get_year_code_quantity_cost(self, _, line):
        line = line.split(',')
        if line[0] != 'Invoice':    # This ensures that the file header is not included
            """The csv (comma-separated value) file has a field (description) which can contain commas.
            This field is thus split into two fields by the split method called above.
            Indexing has therefore been done from the back where necessary to avoid erroneous values
            e.g., selecting the price, quantity and year"""
            stockcode, quantity, price = line[1], float(line[-5]), float(line[-3])
            year = datetime.datetime.strptime(line[-4], '%m/%d/%Y %H:%M:%S').year

            if stockcode: # Don't include customer fields which are blank as these will all be added to one customer: ""
                yield (year, stockcode), (quantity, quantity*price)

    def combiner_sum_quantity_sum_cost(self, year_stock, quantity_cost):
        """
        :param year_stock: tuple which contains year, followed by stockcode
        :param quantity_cost: tuple which contains quantity, followed by cost
        :return: same year_stock as key, and the tuple of the sums of the quantities and costs as value
        """
        quantity_cost_list = list(quantity_cost)
        quantity = sum([row[0] for row in quantity_cost_list])  # Sum the quantities of each 'year, stockcode combination'
        cost = sum([row[1] for row in quantity_cost_list])  # Sum the costs of each 'year, stockcode combination'
        yield year_stock, (quantity, cost)

    def reducer_sum_quantity_sum_cost(self, year_stock, quantity_cost):
        """
        :param year_stock: tuple which contains year, followed by stockcode
        :param quantity_cost: tuple which contains quantity, followed by cost
        :return: same year_stock as key, and the tuple of the sums of the quantities and costs as value
        """
        quantity_cost_list = list(quantity_cost)
        quantity = sum([row[0] for row in quantity_cost_list])  # Sum the quantities of each 'year, stockcode combination'
        cost = sum([row[1] for row in quantity_cost_list])  # Sum the costs of each 'year, stockcode combination'
        yield year_stock, (quantity, cost)

    def mapper_to_years(self, year_stock, quantity_cost):
        """
        :param year_stock: tuple which contains year, followed by stockcode
        :param quantity_cost: tuple which contains quantity, followed by cost
        :return: year as key, (quantity, cost, stockcode) as value
        """
        yield year_stock[0], (quantity_cost[0], quantity_cost[1], year_stock[1]) # Year, (Quantity, cost, stockcode)

    def combiner_max_per_year(self, year, quantity_cost_code):
        """
        :param year: the year
        :param quantity_cost_code: tuple containing quantity, cost and stockcode
        :return: year as key, as value once the tuple (quantity, cost, stockcode) for the item with the highest quantity
        and once for the item with the highest price
        """
        quantity_cost_code_list = list(quantity_cost_code)
        # For each year get the (quantity, cost, stockcode) for the item with the highest quantity
        yield year, max(quantity_cost_code_list, key=operator.itemgetter(0))
        # For each year get the (quantity, cost, stockcode) for the item with the highest cost
        yield year, max(quantity_cost_code_list, key=operator.itemgetter(1))

    def reducer_max_per_year(self, year, quantity_cost_code):
        """
        :param year: the year
        :param quantity_cost_code: tuple containing quantity, cost and stockcode
        :return: year as key, as value once the tuple (quantity, cost, stockcode) for the item with the highest quantity
        and once for the item with the highest price
        """
        quantity_cost_code_list = list(quantity_cost_code)
        # For each year get the (quantity, cost, stockcode) for the item with the highest quantity
        highest_quantity = max(quantity_cost_code_list, key=operator.itemgetter(0))
        # For each year get the (quantity, cost, stockcode) for the item with the highest cost
        highest_cost = max(quantity_cost_code_list, key=operator.itemgetter(1))
        yield year, f"The item that was sold the most was stockcode '{highest_quantity[2]}' of which {int(highest_quantity[0])} items were sold"
        yield year, f"The item that was sold for the most money was stockcode '{highest_cost[2]}', which was sold for ${round(highest_cost[1], 2)}"

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_year_code_quantity_cost,
                   combiner=self.combiner_sum_quantity_sum_cost,
                   reducer=self.reducer_sum_quantity_sum_cost),
            MRStep(mapper=self.mapper_to_years,
                   combiner=self.combiner_max_per_year,
                   reducer=self.reducer_max_per_year)
        ]

if __name__ == '__main__':
    start = time.time()
    MostPopularItems.run()
    print(f'Took {round(time.time()-start, 1)} seconds to finish')