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
    def mapper_code_with_quantity_revenue(_, line):
        line = line.split(',')
        if line[0] != 'Invoice':  # This ensures that the file header is not included
            """
            The csv (comma-separated value) file has a field (description) which can contain commas.
            This field is thus split into two fields by the split method called above.
            Indexing has therefore been done from the back where necessary to avoid erroneous values
            e.g., selecting the price and quantity
            """
            stockcode, quantity, price = line[1], float(line[-5]), float(line[-3])

            if stockcode:  # Don't include empty stockcodes as these will all be added to one item: ""
                yield stockcode, (quantity, quantity*price)

    @staticmethod
    def sum_quantity_sum_revenue(stockcode, quantity_revenue):
        # yield same stockcode as key, and the tuple of the sums of the quantities and revenues as value
        quantity_revenue_list = list(quantity_revenue)
        quantity = sum([row[0] for row in quantity_revenue_list])  # Sum the quantities of each stockcode
        revenue = sum([row[1] for row in quantity_revenue_list])  # Sum the revenues of each stockcode
        yield stockcode, (quantity, revenue)


    @staticmethod
    def mapper_to_None(stockcode, quantity_revenue):
        # yield None, (quantity, revenue, stockcode)
        yield None, (quantity_revenue[0], quantity_revenue[1], stockcode)

    @staticmethod
    def max_per_quantity_and_revenue(_, quantity_revenue_code):
        """
        Yield None as key, as value once the tuple (quantity, revenue, stockcode) for the item with the highest quantity
        and once for the item with the highest price
        """
        quantity_revenue_code_list = list(quantity_revenue_code)
        # Get the (quantity, revenue, stockcode) for the item with the highest quantity
        yield None, max(quantity_revenue_code_list, key=operator.itemgetter(0))
        # Get the (quantity, revenue, stockcode) for the item with the highest revenue
        yield None, max(quantity_revenue_code_list, key=operator.itemgetter(1))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_code_with_quantity_revenue,
                   combiner=self.sum_quantity_sum_revenue,
                   reducer=self.sum_quantity_sum_revenue),
            MRStep(mapper=self.mapper_to_None,
                   combiner=self.max_per_quantity_and_revenue,
                   reducer=self.max_per_quantity_and_revenue)
        ]


if __name__ == '__main__':
    MostPopularItems.run()
