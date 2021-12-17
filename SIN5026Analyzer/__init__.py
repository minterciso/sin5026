import logging
import os
import datetime as dt
import logging.handlers


# Start the logging module

log_base_dir = 'logs'
if os.path.isdir(log_base_dir) is False:
    os.makedirs(log_base_dir)
log_filename = os.path.join(log_base_dir, 'SIN5026Analyzer_{tm}.log'.format(tm=dt.datetime.now().date().strftime('%Y%m%d')))

log = logging.getLogger(__name__)
log.setLevel(level=logging.DEBUG)

formatter_stream_file = logging.Formatter('[*] %(asctime)s - %(name)s - %(levelname)s - %(message)s')
log_stream_handler = logging.StreamHandler()
log_stream_handler.setLevel(logging.INFO)
log_stream_handler.setFormatter(formatter_stream_file)
log_file_handler = logging.FileHandler(log_filename)
log_file_handler.setLevel(logging.DEBUG)
log_file_handler.setFormatter(formatter_stream_file)

log.addHandler(log_stream_handler)
log.addHandler(log_file_handler)
