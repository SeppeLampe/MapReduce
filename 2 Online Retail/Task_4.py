from mrjob.job import MRJob
from mrjob.step import MRStep
import operator
import time

class CustomOutputProtocol:
    # This class will be used to format our output, output needs to be casted to bytes
    def __init__(self):
        self.first = True

    def write(self, key, value):
        if self.first: # We will only yield two things, first the most sold item and then the highest revenue item
            self.first = False
            return bytes(f"Item '{value[2]}' was sold the most: {int(value[0])} times. ", 'utf-8')
        return bytes(f"Item '{value[2]}' had the highest revenue: \u20ac{round(value[1], 2)}.", 'utf-8')

class MostPopularItems(MRJob):
    # Set the output protocol to our own, custom protocol
    OUTPUT_PROTOCOL = CustomOutputProtocol

    @staticmethod
    def mapper_code_with_quantity_cost(_, line):
        line = line.split(',')
        if line[0] != 'Invoice':    # This ensures that the file header is not included
            """
            The csv (comma-separated value) file has a field (description) which can contain commas.
            This field is thus split into two fields by the split method called above.
            Indexing has therefore been done from the back where necessary to avoid erroneous values
            e.g., selecting the price and quantity
            """
            stockcode, quantity, price = line[1], float(line[-5]), float(line[-3])

            if stockcode: # Don't include customer fields which are blank as these will all be added to one customer: ""
                yield stockcode, (quantity, quantity*price)

    @staticmethod
    def sum_quantity_sum_cost(stockcode, quantity_cost):
        # yield same stockcode as key, and the tuple of the sums of the quantities and costs as value
        quantity_cost_list = list(quantity_cost)
        quantity = sum([row[0] for row in quantity_cost_list])  # Sum the quantities of each stockcode
        cost = sum([row[1] for row in quantity_cost_list])  # Sum the costs of each stockcode
        yield stockcode, (quantity, cost)


    @staticmethod
    def mapper_to_None(stockcode, quantity_cost):
        # yield None, (quantity, cost, stockcode)
        yield None, (quantity_cost[0], quantity_cost[1], stockcode)

    @staticmethod
    def max_per_quantity_and_cost(_, quantity_cost_code):
        """
        Yield None as key, as value once the tuple (quantity, cost, stockcode) for the item with the highest quantity
        and once for the item with the highest price
        """
        quantity_cost_code_list = list(quantity_cost_code)
        # Get the (quantity, cost, stockcode) for the item with the highest quantity
        yield None, max(quantity_cost_code_list, key=operator.itemgetter(0))
        # Get the (quantity, cost, stockcode) for the item with the highest cost
        yield None, max(quantity_cost_code_list, key=operator.itemgetter(1))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_code_with_quantity_cost,
                   combiner=self.sum_quantity_sum_cost,
                   reducer=self.sum_quantity_sum_cost),
            MRStep(mapper=self.mapper_to_None,
                   combiner=self.max_per_quantity_and_cost,
                   reducer=self.max_per_quantity_and_cost)
        ]


if __name__ == '__main__':
    start = time.time()
    MostPopularItems.run()
    total_time = int(time.time() - start)
    print(f'Took {total_time//60} minutes and {total_time%60} seconds to finish')
