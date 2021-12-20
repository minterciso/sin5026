import pandas as pd
from numpy.random import rand, randint, choice, uniform
from os import path
from pandas import DataFrame, ExcelWriter
from string import digits
from math import ceil
from time import mktime, strptime
import datetime as dt

from SIN5026Analyzer import log


class RandomGenerator:
    amount_clients = 10
    amount_products = 20
    max_orders_per_clients = 10
    clients_priority_options = [0.05, 0.2, 0.5, 0.8]
    products_priority_options = [0.2, 0.5, 0.9]
    start_date = '2021-01-01'
    end_date = '2021-02-01'
    base_path = None

    __clients = None
    __products = None
    __stock = None
    __orders = None

    @property
    def clients(self):
        return self.__clients

    @property
    def products(self):
        return self.__products

    @property
    def stock(self):
        return self.__stock

    @property
    def orders(self):
        return self.__orders

    def save(self):
        """
        Save everything on a XLSX file as expected for the GA Solver.

        :return: True is saved, False if there's an issue with the base_path
        """
        log.info(f'Saving everything that was generated in folder \'{self.base_path}\'')
        if self.base_path is None:
            log.error('base_path not configured! Unable to save!')
            return False
        self.__to_xlsx()
        log.info('Done')
        return True

    def __to_xlsx(self):
        log.debug('Creating DataFrame and adapting to expected XLSx format...')
        clients_df = DataFrame(self.__clients).rename(columns={'client_id': 'Unique Code', 'client_name': 'Name', 'client_priority': 'Priority'})
        products_df = DataFrame(self.__products).rename(columns={'product_ean': 'EAN', 'product_sku': 'SKU', 'product_name': 'Name', 'product_priority': 'Priority', 'product_net_kg': 'Net Kg', 'product_gross_kg': 'Gross Kg'})
        stock_df = DataFrame(self.__stock).rename(columns={'stock_product_ean': 'EAN', 'stock_product_sku': 'SKU', 'stock_amount': 'Amount'})
        stock_df['Unique Code'] = 'Main Stock'
        stock_df = stock_df[['Unique Code', 'EAN', 'SKU', 'Amount']]
        orders_df = pd.DataFrame(self.__orders).rename(columns={
            'order_id': 'Order ID',
            'order_client_id': 'Client',
            'order_original_date': 'Order Date',
            'order_desired_date': 'Desired Date',
            'order_product_ean': 'EAN',
            'order_product_sku': 'SKU',
            'order_line_idx': 'Line',
            'order_amount': 'Amount'
        })
        orders_df = orders_df[['Order ID', 'Client', 'Line', 'EAN', 'SKU', 'Amount', 'Order Date', 'Desired Date']]
        results_path = path.join(self.base_path, 'results.xlsx')
        log.debug(f'Saving output to \'{results_path}\'...')
        with ExcelWriter(path.normpath(results_path), mode='w') as writer:
            clients_df.to_excel(excel_writer=writer, sheet_name='Clients', index=False)
            products_df.to_excel(excel_writer=writer, sheet_name='Products', index=False)
            stock_df.to_excel(excel_writer=writer, sheet_name='Stock', index=False)
            orders_df.to_excel(excel_writer=writer, sheet_name='Orders', index=False)
        log.debug('Done')

    def create_clients(self):
        """Create sequential clients with random priorities"""
        log.info(f'Creating {self.amount_clients} clients...')
        self.__clients = list()
        for client_id in range(self.amount_clients):
            self.__clients.append({
                'client_id': client_id,
                'client_name': f'Cliente {client_id}',
                'client_priority': choice(self.clients_priority_options)
            })
        log.info('Done')

    def create_products(self):
        """Create sequential products with random priorities"""
        log.info(f'Creating {self.amount_products} products...')
        self.__products = list()
        valid_eans = list()
        for _ in range(ceil(self.amount_products*0.2)):
            valid_eans.append(''.join(choice(list(digits), 12)))
        for prod_idx in range(self.amount_products):
            product_weight = uniform(low=0.2, high=1.8)
            self.__products.append({
                'product_ean': choice(valid_eans),
                'product_sku': ''.join(choice(list(digits), 12)),
                'product_name': f'Random Product #{prod_idx}',
                'product_net_kg': product_weight*0.8,
                'product_gross_kg': product_weight,
                'product_priority': choice(self.products_priority_options)
            })
        log.info('Done')

    def create_random_stock(self):
        """Create a random unique stock for all products"""
        log.info('Creating random Stock values')
        self.__stock = list()
        for product in self.__products:
            self.__stock.append({
                'stock_product_ean': product['product_ean'],
                'stock_product_sku': product['product_sku'],
                'stock_amount': randint(low=0, high=1000)
            })
        log.info('Done')

    def create_random_orders(self):
        """Create random orders"""
        log.info('Creating random orders')
        self.__orders = list()
        for client in self.__clients:
            qtd_orders = randint(low=1, high=self.max_orders_per_clients)
            line_idx = 0
            created_date = int(self.__str_time_prop('%Y-%m-%d', rand()))
            desired_date = int((dt.datetime.fromtimestamp(created_date) + dt.timedelta(days=randint(low=2, high=10))).timestamp())
            for order_id in range(qtd_orders):
                selected_product = choice(self.__products)
                line_idx += 1
                order_amount = randint(low=10, high=1000)
                self.__orders.append({
                    'order_id': f'REQ #{client["client_id"]}',
                    'order_client_id': client['client_id'],
                    'order_original_date': dt.date.fromtimestamp(created_date).strftime('%Y/%m/%d'),
                    'order_desired_date': dt.date.fromtimestamp(desired_date).strftime('%Y/%m/%d'),
                    'order_line_idx': line_idx,
                    'order_product_ean': selected_product['product_ean'],
                    'order_product_sku': selected_product['product_sku'],
                    'order_amount': order_amount,
                })
        log.info('Done')

    def __str_time_prop(self, time_format, prop):
        """Get a time at a proportion of a range of two formatted times.

        start and end should be strings specifying times formatted in the
        given format (strftime-style), giving an interval [start, end].
        prop specifies how a proportion of the interval to be taken after
        start.  The returned time will be in the specified format.
        """

        stime = mktime(strptime(self.start_date, time_format))
        etime = mktime(strptime(self.end_date, time_format))

        ptime = stime + prop * (etime - stime)

        return ptime
