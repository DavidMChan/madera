import logging
import time
import sys

from madera import MaderaHandler

logging.basicConfig(level=logging.DEBUG)
c_handler = MaderaHandler(host='localhost',
                          port=9091,
                          experiment='test',
                          run_id=str(time.time()),
                          api_key=sys.argv[1],
                          level=logging.DEBUG)
logging.getLogger().addHandler(c_handler)
logging.debug('This will get logged')
