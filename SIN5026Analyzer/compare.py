from SIN5026Analyzer.generator import RandomGenerator
from SIN5026Analyzer.ga.solver import Solver as GASolver
from SIN5026Analyzer.ilp.solver import Solver as ILPSolver
from SIN5026Analyzer import log
from solver.algorithms.default.proof import GASelections
from os import path, mkdir
import pandas as pd


def compare(executions_amount: int, clients_amount: int = 40, products_amount: int = 50, max_orders_per_client: int = 10):
    """
    Compare results and return a DataFrame to further analyze it

    :param executions_amount: The amount of executions to compare
    :param clients_amount: The amount of clients to create
    :param products_amount: The amount of products to request
    :param max_orders_per_client: The amount of maximum lines for each order request
    :return: A DataFrame with a summary of sent/missing of each execution
    """
    log.info(f'======COMPARING {executions_amount} RANDOM EXECUTIONS======')
    comparable_results = pd.DataFrame()

    for execution_id in range(executions_amount):
        log.info(f'=====Execution {execution_id}')
        rng = RandomGenerator()
        rng.amount_clients = clients_amount
        rng.amount_products = products_amount
        rng.max_orders_per_clients = max_orders_per_client
        rng.create_clients()
        rng.create_products()
        rng.create_random_stock()
        rng.create_random_orders()
        rng.base_path = '/tmp/sin5026_asdrubal'
        if path.isdir(rng.base_path) is False:
            mkdir(rng.base_path)
        rng.save()

        ga_solver = GASolver(path.join(rng.base_path, 'results.xlsx'))
        ga_solver.instant_configuration['tournament_size'] = 16
        ga_solver.instant_configuration['selections'] = GASelections.TOURNAMENT
        ga_solver.instant_configuration['population_size'] = 512 * 5
        ga_solver.instant_configuration['max_generations'] = 100
        ga_solver.solve()
        ga_solver.results_df['missing'].sum()

        base_path = path.join(ga_solver.temp_path)
        orders_path = path.join(base_path, 'orders.csv')
        clients_path = path.join(base_path, 'clients.csv')
        products_path = path.join(base_path, 'products.csv')
        stock_path = path.join(base_path, 'stock.csv')

        ilp_solver = ILPSolver(use_sample=False, orders_path=orders_path, clients_path=clients_path, products_path=products_path, stock_path=stock_path)
        ilp_solver.solve(False)
        ilp_solver.results_df['missing'].sum()
        ga_solver.cleanup()
        ga_df = ga_solver.results_df.copy()
        ilp_df = ilp_solver.results_df.copy()
        cmp_df = ga_df.join(ilp_df, lsuffix='_ga').drop(['client_ga', 'product_ga', 'requested_ga'], axis=1)[[
            'client', 'product', 'requested', 'sent_ga', 'missing_ga', 'sent', 'missing']].rename(columns={'sent': 'sent_ilp', 'missing': 'missing_ilp'})
        cmp_df['product_priority'] = 0.0
        cmp_df['client_priority'] = 0.0
        cmp_df['execution'] = execution_id
        client_df = pd.DataFrame(rng.clients)
        for idx, row in client_df.iterrows():
            cl_priority = row['client_priority']
            cl_name = row['client_name']
            cmp_df['client_priority'] = cmp_df.apply(lambda x: cl_priority if cl_name == x['client'] else x['client_priority'], axis=1)
        products_df = pd.DataFrame(rng.products)
        products_df['product'] = products_df['product_ean'] + '-' + products_df['product_sku']
        for idx, row in products_df.iterrows():
            product_priority = row['product_priority']
            product = row['product']
            cmp_df['product_priority'] = cmp_df.apply(lambda x: product_priority if product == x['product'] else x['product_priority'], axis=1)
        comparable_results = pd.concat([comparable_results, cmp_df])
        ga_solver.cleanup()
    log.info('======DONE======')
    return comparable_results


