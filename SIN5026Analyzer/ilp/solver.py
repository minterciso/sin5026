from gurobipy import Model, GRB, multidict, Env
import pandas as pd
from SIN5026Analyzer import log


class Solver:
    # Control variables
    use_sample = True

    # Solver variables and data information
    orders = None
    requested = None
    priority = None
    stock = None
    available = None
    products = None
    clients = None
    # Solver specific
    model = None
    x = None
    missing = None
    constraints = list()
    __results_df = None

    @property
    def results_df(self):
        return self.__results_df

    def __init__(self, use_sample: bool = True, orders_path: str = None, clients_path: str = None, products_path: str = None, stock_path: str = None):
        """
        Configure the initial setup for the solver (where to get the data)

        :param use_sample: If this is set to True, it'll create a very small sample data
        :param orders_path: The orders.csv path as created by the generator
        :param clients_path: The clients.csv path as created by the generator
        :param products_path: the products.csv path as created by the generator
        :param stock_path: the stock.csv path as created by the generator
        """
        self.use_sample = use_sample
        if self.use_sample:
            self.__create_sample()
        else:
            self.__read_csv(orders_path, clients_path, products_path, stock_path)

    def solve(self, verbose: bool = False):
        """
        Actually tries to solve the problem via ILP

        :param verbose: If set to True, it'll output the default Gurobi log on the terminal, if set to False it'll suppress the output

        :todo: Add a detailed Markdown description of the solution
        """
        log.info('Solving via Gurobi')
        log.debug('Configuring the Environment')
        env = Env(empty=True)
        if verbose:
            env.setParam('OutputFlag', 1)
        else:
            env.setParam('OutputFlag', 0)
        env.start()
        log.debug('Creating the model')
        self.model = Model('SIN5026', env=env)
        self.__configure_solver()
        log.debug('Optimizing')
        self.model.optimize()
        log.debug('Creating results DataFrame')
        self.__create_results_df()
        log.info('Done')

    def __configure_solver(self):
        """Configure the solver by creating the variables, the constraints and the objective"""
        log.info('Configuring solver')
        log.debug('Creating variables')
        self.x = self.model.addVars(self.orders, name='orders')
        self.missing = self.model.addVars(self.orders, name='missing')  # I don't believe that this is useful, maybe remove it?
        log.debug('Adding constraints')
        self.constraints.append(self.model.addConstrs((self.x.sum('*', key) + self.missing.sum('*', key) <= value for key, value in self.available.items()),
                                                      'stock'))
        name_idx = -1
        for product in self.products:
            for idx in range(len(self.clients)):
                name_idx += 1
                self.constraints.append(
                    self.model.addConstr(
                        self.x.select('*', product)[idx] + self.missing.select('*', product)[idx] <= self.requested.select('*', product)[idx],
                        name=f'requested_{name_idx}'
                    )
                )
        log.debug('Configuring objective')
        self.model.setObjective(self.x.prod(self.priority) - self.missing.prod(self.priority), GRB.MAXIMIZE)
        log.info('Done')

    def __create_results_df(self):
        """Create a results Panda DataFrame to use it for comparing the solution with the GA approach"""
        log.info('Creating Pandas DataFrame to analyze results')
        to_send = list()
        for v in self.model.getVars():
            var_type = v.varName.split('[')[0]
            if var_type == 'orders':
                keys = v.varName.split('[')[1].split(',')
                value = v.x
                total_requested = self.requested.select(keys[0], keys[1][0:-1])[0]
                total_missing = total_requested - value
                to_send.append(
                    {
                        'client': keys[0],
                        'product': keys[1][0:-1],
                        'sent': value,
                        'requested': total_requested,
                        'missing': total_missing
                    }
                )
        self.__results_df = pd.DataFrame(to_send)

    def __read_csv(self, orders_path: str, clients_path: str, products_path: str, stock_path: str):
        """
        Read the information on the CSV files created by the generator

        :param orders_path: The orders.csv path as created by the generator
        :param clients_path: The clients.csv path as created by the generator
        :param products_path: the products.csv path as created by the generator
        :param stock_path: the stock.csv path as created by the generator
        """
        log.info('Reading information from files:')
        log.info(f'- Clients  : {clients_path}')
        log.info(f'- Products : {products_path}')
        log.info(f'- Stock    : {stock_path}')
        log.info(f'- Orders   : {orders_path}')
        products_df = pd.read_csv(products_path)
        stock_df = pd.read_csv(stock_path)
        clients_df = pd.read_csv(clients_path)
        orders_df = pd.read_csv(orders_path)
        log.debug('Cleaning up data...')
        # Cleaning all CSVs and joining for ease of working on
        stock_df.rename(columns={'stock_product_ean': 'product_ean', 'stock_product_sku': 'product_sku'}, inplace=True)
        joined_products_df = products_df.set_index(['product_ean', 'product_sku']).join(stock_df.set_index(['product_ean', 'product_sku'])).reset_index()
        joined_products_df['product'] = joined_products_df['product_ean'].astype(str) + '-' + joined_products_df['product_sku'].astype(str)
        cleaned_orders_df = orders_df[['order_client_id', 'order_id', 'order_priority', 'order_product_ean', 'order_product_sku', 'order_amount']].rename(columns={
            'order_client_id': 'client_id'
        })
        cleaned_orders_df['product'] = cleaned_orders_df['order_product_ean'].astype(str) + '-' + cleaned_orders_df['order_product_sku'].astype(str)
        complete_cleaned_orders_df = cleaned_orders_df.merge(clients_df, left_on='client_id', right_on='client_id')[['order_id', 'order_priority', 'product', 'order_amount', 'client_name']]
        # Add missing data
        log.debug('Adding missing data (in order to work with LP, we need to have a full data configured)')
        products_lst = joined_products_df['product'].to_list()
        grouped_orders = complete_cleaned_orders_df.groupby('order_id')
        for g in grouped_orders.groups:
            req = grouped_orders.get_group(g)
            all_products = req['product'].to_list()
            missing_products = list(set(products_lst) - set(all_products))
            missing_data = {
                'order_id': [req['order_id'].to_list()[0]] * len(missing_products),
                'order_priority': [req['order_priority'].to_list()[0]] * len(missing_products),
                'product': missing_products,
                'order_amount': [0] * len(missing_products),
                'client_name': [req['client_name'].to_list()[0]] * len(missing_products),
            }
            complete_cleaned_orders_df = complete_cleaned_orders_df.append([pd.DataFrame(missing_data)])
        # Creating gurobi multidicts we need
        log.debug('Creating dictionaries with Gurobi multidict helper')
        orders_dct = dict()
        for row in complete_cleaned_orders_df[['client_name', 'product', 'order_priority', 'order_amount']].itertuples():
            tup_idx = (row[1], row[2])
            lst_vals = [row[4], row[3]]
            orders_dct[tup_idx] = lst_vals
        Orders, requested, priority = multidict(orders_dct)
        stock_dct = dict()
        for row in joined_products_df[['product', 'stock_amount']].itertuples():
            stock_dct[row[1]] = row[2]
        stock, available = multidict(stock_dct)
        log.debug('Storing in memory')
        self.orders = Orders
        self.requested = requested
        self.priority = priority
        self.stock = stock
        self.available = available
        self.products = joined_products_df['product'].to_list()
        self.clients = clients_df['client_name'].to_list()
        log.info('Done')

    def __create_sample(self):
        """Create a very small sample data"""
        log.info('Creating a very small in memory sample, just to validate the solver.')
        self.products = ['ProductA', 'ProductB', 'ProductC']
        self.clients = ['ClientA', 'ClientB', 'ClientC']
        self.orders, self.requested,self. priority = multidict({
            ('ClientA', 'ProductA'): [80, 0.4923],
            ('ClientA', 'ProductB'): [20, 0.4923],
            ('ClientA', 'ProductC'): [0, 0.4923],
            ('ClientB', 'ProductA'): [30, 0.3538],
            ('ClientB', 'ProductB'): [40, 0.3538],
            ('ClientB', 'ProductC'): [5, 0.3538],
            ('ClientC', 'ProductA'): [10, 0.1538],
            ('ClientC', 'ProductB'): [0, 0.1538],
            ('ClientC', 'ProductC'): [10, 0.1538]
        })
        self.stock, self.available = multidict({
            ('ProductA'): 100,
            ('ProductB'): 50,
            ('ProductC'): 10
        })

