#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import sys
import asyncio
# import json
# import pandas as pd
import argparse
import yaml
import textwrap
# import time
# import os.path
import requests
from bscscan import BscScan

import multiprocessing
import http.server
import socketserver
from termcolor import colored
import coloredlogs, logging

# from web3_input_decoder import InputDecoder, decode_constructor

from api_poorSKeme import create_application
from core import bsc


# create a logger object.
logger = logging.getLogger(__name__)


__author__ = "KennBro"
__copyright__ = "Copyright 2022, Personal Research"
__credits__ = ["KennBro"]
__license__ = "GPL"
__version__ = "1.0.0"
__maintainer__ = "KennBro"
__email__ = "kennbro <at> protonmail <dot> com"
__status__ = "Development"


def flaskServer(ip='127.0.0.1', port=5000):
    app = create_application()
    # app.run(host=ip, port=port, debug=True)
    logger.info("Flask serving...")
    app.run(port=port, debug=True, host=ip, use_reloader=False)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="./frontend/dist/", **kwargs)


def httpServer():
    PORT = 4200
    logger.info("HTTPD serving... Data Visualization Web on http://127.0.0.1:4200")
    with socketserver.TCPServer(("", PORT), Handler) as httpd_server:
        httpd_server.serve_forever()


if __name__ == "__main__":

    print("""
$$$$$$$\                                       $$$$$$\  $$\   $$\                                  
$$  __$$\                                     $$  __$$\ $$ | $$  |                                 
$$ |  $$ | $$$$$$\   $$$$$$\   $$$$$$\        $$ /  \__|$$ |$$  / $$$$$$\  $$$$$$\$$$$\   $$$$$$\  
$$$$$$$  |$$  __$$\ $$  __$$\ $$  __$$\       \$$$$$$\  $$$$$  / $$  __$$\ $$  _$$  _$$\ $$  __$$\ 
$$  ____/ $$ /  $$ |$$ /  $$ |$$ |  \__|       \____$$\ $$  $$<  $$$$$$$$ |$$ / $$ / $$ |$$$$$$$$ |
$$ |      $$ |  $$ |$$ |  $$ |$$ |            $$\   $$ |$$ |\$$\ $$   ____|$$ | $$ | $$ |$$   ____|
$$ |      \$$$$$$  |\$$$$$$  |$$ |            \$$$$$$  |$$ | \$$\\$$$$$$$\ $$ | $$ | $$ |\$$$$$$$\ 
\__|       \______/  \______/ \__|             \______/ \__|  \__|\_______|\__| \__| \__| \_______|

    """)

    # Parse arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            Contract analyzer
            '''),
        epilog='''
            Examples
            --------

            # Extract TRXs of contract from block to block
            python3 poorSKeme.py -ct 0xe878BccA052579C9061566Cec154B783Fc5b9fF1 -bf 14040726 -bt 15552901 # To collect

            # Data Visualization of processed contract information
            python3 poorSKeme.py -f F/contract-0xe878BccA052579C9061566Cec154B783Fc5b9fF1.json -w
            ''')

    group1 = parser.add_argument_group("Get and process data")
    group2 = parser.add_argument_group("Process data from JSON file")
    group3 = parser.add_argument_group("Start WebServer visualization data")

    group1.add_argument('-c', '--chunk', type=int, default=10000,
                        help='Chunks of blocks')
    group1.add_argument('-ct', '--contract',
                        help="address of contract")
    group1.add_argument('-bf', '--block-from', default=0, type=int,
                        help="Block start")
    group1.add_argument('-bt', '--block-to', default=9999999, type=int,
                        help="Block end")

    group2.add_argument('-f', '--file', type=str,
                        help="JSON file of recolected data")

    group3.add_argument('-w', '--web', action='store_true',
                        help="WEB for data visaulization")

    args = parser.parse_args()

    # Read config and keys
    with open(r'./API.yaml') as file:
        key = yaml.load(file, Loader=yaml.FullLoader)

    coloredlogs.install(level='INFO')
    try:
        coloredlogs.install(level=key['level'])
    except Exception:
        logger.error("The level parameter is worng, set to INFO by default")
        coloredlogs.install(level='INFO')


    # Validations
    if (args.contract):
        # asyncio.run(save_json(args.contract, args.block_from, args.block_to, key['bscscan'], chunk=args.chunk))
        asyncio.run(bsc.bsc_json_collect(args.contract, args.block_from, args.block_to, key['bscscan'], chunk=args.chunk))
        if (args.file):
            logger.error("Parameter JSON file are discarded because contract is provided")

    elif (args.file):
        rc = bsc.bsc_json_process(args.file)

    else:
        if (args.block_from or args.block_to or args.chunck):
            logger.error("The CONTRACT ADDRESS is not specified")
        # parser.print_help(sys.stderr)

    if (args.web):
        sys.stdout.flush()
        kwargs_flask = {"ip": "127.0.0.1", "port": 5000}
        flask_proc = multiprocessing.Process(name='flask',
                                                target=flaskServer,
                                                kwargs=kwargs_flask)
        flask_proc.daemon = True

        sys.stdout.flush()
        httpd_proc = multiprocessing.Process(name='httpd',
                                                target=httpServer)
        httpd_proc.daemon = True

        flask_proc.start()
        httpd_proc.start()
        flask_proc.join()
        httpd_proc.join()
