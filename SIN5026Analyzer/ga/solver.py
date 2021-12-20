from copy import deepcopy
from os import path, mkdir
from tempfile import TemporaryDirectory

import pandas as pd
from solver.algorithms.default.runner import Loader, Run
from solver.algorithms.default.proof import ProofGA, GASelections
from SIN5026Analyzer import log


class Solver:
    loader = None
    runner = None
    instant_configuration = {
        'prob_xover': 0.6,
        'prob_mutation': 0.005,
        'mutation_prob_amount': 0.4,
        'elite_amount': 0.2,
        'tournament_size': 4,
        'population_size': 100,
        'max_generations': 100,
        'selections': (GASelections.ELITE | GASelections.TOURNAMENT)

    }
    __temp_dir = None
    __data_file_path = None
    __results_file_path = None
    __results_df = None

    @property
    def results_df(self):
        return self.__results_df

    @property
    def base_dir(self):
        return self.__temp_dir.name

    def __init__(self, data_file_path: str):
        self.__data_file_path = data_file_path
        self.__temp_dir = TemporaryDirectory()
        self.__results_file_path = path.join(self.__temp_dir.name, 'results.xlsx')
        self.__read_csv()
        self.runner = Run()

    def solve(self):
        log.info('Starting GA solver')
        log.debug('Configuring directories')
        self.runner.base_dir = self.__temp_dir.name
        self.runner.cuda_base_dir = self.__temp_dir.name
        log.debug('Configuring data files')
        self.runner.data_file = self.__data_file_path
        self.runner.products = deepcopy(self.loader.products)
        self.runner.orders = deepcopy(self.loader.orders)
        self.runner.clients = deepcopy(self.loader.clients)
        self.runner.stock = deepcopy(self.loader.stock)
        log.debug('Configuring runner type (CUDA)')
        self.runner.use_numba = False
        self.runner.use_cuda = True
        self.__configure_solver()
        log.debug('Starting GA solution')
        time, best = self.runner.start('ga_comparison')
        log.debug(f'Finished in {time} seconds, saving best solution found')
        self.runner.save_result_output(self.__results_file_path)
        log.info(f'Finished evolution:')
        log.info(f'- Solution file: {self.__results_file_path}')
        log.info(f'- Time took: {time}')
        log.info('Creating results DataFrame')
        self.__create_results_df()
        log.info('Done')

    def cleanup(self):
        """Remove the temporary directory and it's content"""
        self.__temp_dir.cleanup()

    def __configure_solver(self):
        """Configure the GA based on the instant_configuration dictionary"""
        log.debug('Configuring GA with:')
        if self.instant_configuration is not None:
            log.debug(f'- Crossover Probability      : {self.instant_configuration["prob_xover"]}')
            log.debug(f'- Mutation Probability       : {self.instant_configuration["prob_mutation"]}')
            log.debug(f'- Mutation Probability Amount: {self.instant_configuration["mutation_prob_amount"]}')
            log.debug(f'- Elite Amount               : {self.instant_configuration["elite_amount"]}')
            log.debug(f'- Tournament Size            : {self.instant_configuration["tournament_size"]}')
            log.debug(f'- Population Size            : {self.instant_configuration["population_size"]}')
            log.debug(f'- Generations Amount         : {self.instant_configuration["max_generations"]}')
            log.debug(f'- Selection Type             : {self.instant_configuration["selections"]}')
            self.runner.setup(
                pc=self.instant_configuration['prob_xover'],
                pm=self.instant_configuration['prob_mutation'],
                mpa=self.instant_configuration['mutation_prob_amount'],
                ea=self.instant_configuration['elite_amount'],
                ts=self.instant_configuration['tournament_size'],
                ps=self.instant_configuration['population_size'],
                max_generations=self.instant_configuration['max_generations'],
                selections=self.instant_configuration['selections']
            )
        else:
            log.debug(f'- instant_configuration is None, configuring with database configuration id 1')
            self.runner.setup(configuration_id=1)
        log.debug('Done')

    def __create_results_df(self):
        """Create a results Panda DataFrame to use it for comparing the solution with the ILP approach"""
        log.debug('Reading XLSX file')
        self.__results_df = pd.read_excel(self.__results_file_path, usecols=['Client ID', 'EAN', 'SKU', 'Total Requested', 'Total Shipped']).rename(columns={
            'Client ID': 'client id',
            'Total Requested': 'requested',
            'Total Shipped': 'sent'}
        )
        log.debug('Cleaning up')
        self.__results_df['client'] = self.__results_df['client id'].apply(lambda x: f'Client {x}')
        self.__results_df['product'] = self.__results_df.apply(lambda x: f'{x["EAN"]}-{x["SKU"]}', axis=1)
        self.__results_df.drop(['client id', 'EAN', 'SKU'], axis=1, inplace=True)
        self.__results_df['missing'] = self.__results_df['requested'] - self.__results_df['sent']
        self.__results_df = self.__results_df[['client', 'product', 'requested', 'sent', 'missing']].sort_values(by=['client', 'product'])
        self.__results_df.reset_index(drop=True, inplace=True)

    def __read_csv(self):
        """Read the Generated XLSx file, and store all the CSV in a temporary directory"""
        log.debug(f'Reading base XLSx data file \'{self.__data_file_path}\'')
        self.loader = Loader()
        self.loader.xls_file = self.__data_file_path
        self.loader.load()
        log.debug(f'Saving CSV sheets from XLSx data file \'{self.__data_file_path}\'')
        self.loader.save_to_csv(self.__temp_dir.name)
        log.debug('Done')

