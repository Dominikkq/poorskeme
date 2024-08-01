#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import json
import re
import numpy as np
import pandas as pd
import time
import os.path
from pandas.core.series import unpack_1tuple
import requests
import sqlite3
import asyncio
import aiohttp
import pyround
import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
import logging

from web3_input_decoder import InputDecoder # , decode_constructor


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


__author__ = "KennBro"
__copyright__ = "Copyright 2023, Personal Research"
__credits__ = ["KennBro"]
__license__ = "GPL"
__version__ = "1.0.1"
__maintainer__ = "KennBro"
__email__ = "kennbro <at> protonmail <dot> com"
__status__ = "Development"

async def aio_db_transactions(conn, table, transactions):    
    logger.info(f"Processing - TRANSACTIONS")

    rows = []
    for json_object in transactions:
        rows.append((json_object['block'],
                     json_object['timestamp'],
                     json_object['hash'],
                     json_object['nonce'],
                     json_object['fee']['value'],
                     json_object['gas_limit'],
                     json_object['gas_price'],
                     json_object['gas_used'],
                     json_object['method'],
                     json_object['from']['hash'],
                     json_object['to']['hash'],
                     json_object['value'],
                     json_object['status'],
                     json_object['confirmations'],
                     json_object['decoded_input']['method_call'] if json_object['decoded_input'] else None,
                     json_object['decoded_input']['method_id'] if json_object['decoded_input'] else None,
                     json_object['decoded_input']['parameters'] if json_object['decoded_input'] else None,
                     json_object['result'],
                     json_object['tx_types'][0] if json_object['tx_types'] else None
                    ))

    if rows:
        conn.executemany(f"""INSERT INTO {table} VALUES 
                         (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         """, rows)
        conn.commit()


async def aio_db_transfers(client_session, url, conn, table):    
    startblock = url.split("startblock=")[1].split("&endblock=")[0]
    endblock = url.split("endblock=")[1].split("&sort=")[0]
    logger.info(f"Processing - TRANSFERS from {startblock} to {endblock}")

    async with client_session.get(url) as resp:
        data = await resp.json()

        rows = []
        for json_object in data['result']:

            rows.append((json_object['blockNumber'],
                 json_object['timeStamp'],
                 json_object['hash'],
                 json_object['nonce'],
                 json_object['blockHash'],
                 json_object['from'],
                 json_object['contractAddress'],
                 json_object['to'],
                 json_object['value'],
                 json_object['tokenName'],
                 json_object['tokenSymbol'],
                 json_object['tokenDecimal'],
                 json_object['transactionIndex'],
                 json_object['gas'],
                 json_object['gasPrice'],
                 json_object['gasUsed'],
                 json_object['cumulativeGasUsed'],
                 json_object['input'],
                 json_object['confirmations']))

        if (rows != []):
            conn.executemany(f"""INSERT INTO {table} VALUES 
                             (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                             ?, ?, ?, ?, ?, ?, ?, ?, ?)
                             """,rows)
            conn.commit()


async def aio_db_internals(client_session, url, conn, table):    
    startblock = url.split("startblock=")[1].split("&endblock=")[0]
    endblock = url.split("endblock=")[1].split("&sort=")[0]
    logger.info(f"Processing - INTERNALS from {startblock} to {endblock}")

    async with client_session.get(url) as resp:
        data = await resp.json()

        for json_object in data['result']:
            conn.execute(f"""INSERT INTO {table} VALUES 
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                           """,
                (json_object['blockNumber'],
                 json_object['timeStamp'],
                 json_object['hash'],
                 json_object['from'],
                 json_object['to'],
                 json_object['value'],
                 json_object['contractAddress'],
                 json_object['input'],
                 json_object['type'],
                 json_object['gas'],
                 json_object['gasUsed'],
                 json_object['isError'],
                 json_object['errCode']))

        conn.commit()


def fetch_transactions(contract_address):
    base_url = f'https://rootstock.blockscout.com/api/v2/addresses/{contract_address}/transactions'
    logger.info("Starting to retrieve transactions for the contract creator")

    transactions = []
    first_transaction_block = None
    last_transaction_block = None
    last_transaction = {}
    first_transaction = {}
    first_time = None
    last_time = None
    has_more_pages = True
    params = {}  # Initially, no parameters are needed for the first API request
    fetch_count = {}  # To count fetches for each set of parameters

    while has_more_pages:
        try:
            logger.info(f"Requesting {base_url} with params {params}")
            response = requests.get(base_url, params=params, timeout=15)  # 15-second timeout
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            data = response.json()

            batch_transactions = data['items']
            logger.debug(f"Received {len(batch_transactions)} transactions")

            if not batch_transactions:
                logger.info("No transactions received, ending pagination.")
                break

            current_params_signature = frozenset(params.items())
            fetch_count[current_params_signature] = fetch_count.get(current_params_signature, 0) + 1

            # Stop if the same data page is fetched more than twice
            if fetch_count[current_params_signature] > 2:
                logger.info("Repeated data fetch detected, stopping pagination.")
                break

            transactions.extend(batch_transactions)

            for transaction in batch_transactions:
                if last_transaction_block is None or transaction['block'] < last_transaction_block:
                    last_transaction = transaction
                    last_transaction_block = transaction['block']
                    last_time = datetime.datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))
                if first_transaction_block is None or transaction['block'] > first_transaction_block:
                    first_transaction = transaction
                    first_transaction_block = transaction['block']
                    first_time = datetime.datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))

            if 'next_page_params' in data and data['next_page_params']:
                params = data['next_page_params']
            else:
                has_more_pages = False
                logger.debug("No more pages to fetch")

        except requests.Timeout:
            logger.error("Request timed out, retrying...")
            continue

        except requests.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            break

        except requests.RequestException as req_err:
            logger.error(f"Request error occurred: {req_err}")
            break

        except ValueError as json_err:
            logger.error(f"JSON decoding failed: {json_err}")
            break

    logger.info(f"First transaction block: {first_transaction_block}, Timestamp: {first_time}")
    logger.info(f"Last transaction block: {last_transaction_block}, Timestamp: {last_time}")
    return transactions

async def async_fetch_and_store(contract_address, conn, table):
    logger.info("Starting async fetch and store")
    transactions = fetch_transactions(contract_address)
    await aio_db_transactions(conn, table, transactions)

def rsk_db_collect_async(contract_address, block_from, block_to, key, filedb, chunk=30000):
    
    try:
        os.remove(filedb)
    except:
        pass
    logger.info(f"Creating db {filedb}")
    connection = sqlite3.connect(filedb)
    cursor = connection.cursor()

    # Internal tagging
    internal_tagging = []
    sql_create_tagging_table = """CREATE TABLE IF NOT EXISTS t_tagging (
                                   wallet text NOT NULL,
                                   tag text NOT NULL
                                 );"""
    cursor.execute(sql_create_tagging_table)

    logger.info("=====================================================")
    logger.info("Collecting Contract data")
    logger.info("=====================================================")
    logger.info("Creating Table t_contract")

    sql_create_contract_table = """CREATE TABLE IF NOT EXISTS t_contract (
                                   contract text NOT NULL,
                                   blockchain text NOT NULL,
                                   block_from text NOT NULL,
                                   block_to text NOT NULL,
                                   first_block text NOT NULL,
                                   transaction_creation text NOT NULL,
                                   date_creation datetime NOT NULL,
                                   creator text NOT NULL
                                 );"""
    cursor.execute(sql_create_contract_table)

    logger.info("Getting first block")
    url = 'https://rootstock.blockscout.com/api/v2/addresses/' + contract_address + "/transactions"

    response = requests.get(url)
        
    first_block = response.json()['items'][-1]

    if (block_from == 0):
        block_from = int(first_block['block'])
    contract_creator = first_block['from']['hash']
    logger.info("FROM URL: " + url)
    logger.info(first_block)
    logger.info("Contract Creator: " + contract_creator)
    logger.info("Storing first block")
    cursor.execute("""INSERT INTO t_contract VALUES (?, ?, ?, ?, ?, ?, ?, ?)""", 
        (contract_address, 'rsk', block_from, block_to, first_block['block'], 
        first_block['hash'], first_block['timestamp'], first_block['from']['hash']))

    connection.commit()
    # tag
    internal_tagging.append({"wallet": contract_address, "tag": "Contract Analysed"})
    internal_tagging.append({"wallet": first_block['from'], "tag": "Contract Creator"})

    # Source code
    logger.info("Creating Table t_source_abi")

    sql_create_source_abi_table = """CREATE TABLE IF NOT EXISTS t_source_abi (
                                     SourceCode text NOT NULL,
                                     ABI text NOT NULL,
                                     ContractName text NOT NULL,
                                     CompilerVersion text NOT NULL,
                                     OptimizationUsed text NOT NULL,
                                     Runs text NOT NULL,
                                     ConstructorArguments text NOT NULL,
                                     EVMVersion text NOT NULL,
                                     Library text NOT NULL,
                                     LicenseType text NOT NULL,
                                     Proxy text NOT NULL,
                                     Implementation text NOT NULL,
                                     SwarmSource text NOT NULL
                                   );"""
    cursor.execute(sql_create_source_abi_table)

    logger.info("Getting source code and ABI")
    url = 'https://rootstock.blockscout.com/api/v2/smart-contracts/' + contract_address
    response = requests.get(url)
    json_obj_source_code = response.json()

    logger.info("Storing source code and ABI")


    # Log the values before inserting
    source_code_value = json_obj_source_code.get('source_code', '')
    abi_value = json.dumps(json_obj_source_code.get('abi', {}))
    contract_name_value = json_obj_source_code.get('name', '')
    compiler_version_value = json_obj_source_code.get('compiler_version', '')
    optimization_used_value = str(json_obj_source_code.get('optimization_enabled', 'false'))
    runs_value = json_obj_source_code.get('optimization_runs') if json_obj_source_code.get('optimization_runs') is not None else 0
    constructor_arguments_value = json.dumps(json_obj_source_code.get('constructor_args', {}))
    evm_version_value = json_obj_source_code.get('evm_version', '')
    library_value = json.dumps(json_obj_source_code.get('external_libraries', {}))
    license_type_value = json_obj_source_code.get('license_type', '')
    proxy_value = str(json_obj_source_code.get('has_methods_write_proxy', 'false'))
    implementation_value = "0x"
    swarm_source_value = ''

    # Log each value to identify any issues
    logger.info(f"ContractName: {contract_name_value}")
    logger.info(f"CompilerVersion: {compiler_version_value}")
    logger.info(f"OptimizationUsed: {optimization_used_value}")
    logger.info(f"Runs: {runs_value}")
    logger.info(f"ConstructorArguments: {constructor_arguments_value}")
    logger.info(f"EVMVersion: {evm_version_value}")
    logger.info(f"Library: {library_value}")
    logger.info(f"LicenseType: {license_type_value}")
    logger.info(f"Proxy: {proxy_value}")
    logger.info(f"Implementation: {implementation_value}")
    logger.info(f"SwarmSource: {swarm_source_value}")

    # Ensure runs_value is an integer
    runs_value = int(runs_value)

    # Execute the insertion with all values
    cursor.execute("""
    INSERT INTO t_source_abi (
        SourceCode,
        ABI,
        ContractName,
        CompilerVersion,
        OptimizationUsed,
        Runs,
        ConstructorArguments,
        EVMVersion,
        Library,
        LicenseType,
        Proxy,
        Implementation,
        SwarmSource
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
        source_code_value,
        abi_value,
        contract_name_value,
        compiler_version_value,
        optimization_used_value,
        runs_value,
        constructor_arguments_value,
        evm_version_value,
        library_value,
        license_type_value,
        proxy_value,
        implementation_value,
        swarm_source_value
    ))


    connection.commit()
    # connection.close()

    logger.info("Building URLs")
    startblock = block_from
    endblock = block_from + chunk

    urls_transactions = []
    urls_internals = []
    urls_transfers = []
    urls_trx_creator = []
    urls_int_creator = []
    urls_tra_creator = []
    while startblock < block_to:
        urls_transactions.append('https://api.bscscan.com/api?module=account&action=txlist&address=' + contract_address + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        urls_internals.append('https://api.bscscan.com/api?module=account&action=txlistinternal&address=' + contract_address + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        urls_transfers.append('https://api.bscscan.com/api?module=account&action=tokentx&address=' + contract_address + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        urls_trx_creator.append('https://api.bscscan.com/api?module=account&action=txlist&address=' + contract_creator + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        urls_int_creator.append('https://api.bscscan.com/api?module=account&action=txlistinternal&address=' + contract_creator + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        urls_tra_creator.append('https://api.bscscan.com/api?module=account&action=tokentx&address=' + contract_creator + \
                  '&startblock=' + str(startblock) + '&endblock=' + str(endblock) + '&sort=asc&apikey=' + key)
        startblock += chunk + 1
        endblock += chunk + 1

    logger.info(" ")
    logger.info("=====================================================")
    logger.info("Collecting Contract transactions")
    logger.info("=====================================================")
    logger.info("Creating Table t_transactions")

    sql_create_transactions_table = """CREATE TABLE IF NOT EXISTS t_transactions (
                                       blockNumber integer NOT NULL,
                                       timeStamp datetime NOT NULL,
                                       hash text NOT NULL,
                                       nonce integer NOT NULL,
                                       blockHash text NOT NULL,
                                       transactionIndex integer NOT NULL,
                                       `from` text NOT NULL,
                                       `to` text NOT NULL,
                                       value integer NOT NULL,
                                       gas integer NOT NULL,
                                       gasPrice integer NOT NULL,
                                       isError integer NOT NULL,
                                       txreceipt_status integer NOT NULL,
                                       input text NOT NULL,
                                       contractAddress text NOT NULL,
                                       cumulativeGasUsed integer NOT NULL,
                                       gasUsed integer NOT NULL,
                                       confirmations integer NOT NULL,
                                       methodId text NOT NULL,
                                       functionName text NOT NULL
                                 );"""
    connection.execute(sql_create_transactions_table)

    logger.info("Getting transactions async")
    start_time = time.time()
    asyncio.run(async_fetch_and_store(contract_address, connection, "t_transactions"))
    end_time = time.time()
    
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_transactions) / elapsed_time

    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_transactions"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_transactions)}")
    logger.info(f" Total Blocks: {len(urls_transactions) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")

    logger.info(" ")
    logger.info("=====================================================")
    logger.info(f"Collecting Contract transfers...")
    logger.info("=====================================================")
    logger.info("Creating Table t_transfers")

    sql_create_transfers_table = """CREATE TABLE IF NOT EXISTS t_transfers (
                                       blockNumber integer NOT NULL,
                                       timeStamp datetime NOT NULL,
                                       hash text NOT NULL,
                                       nonce integer NOT NULL,
                                       blockHash text NOT NULL,
                                       `from` text NOT NULL,
                                       contractAddress text NOT NULL,
                                       `to` text NOT NULL,
                                       value integer NOT NULL,
                                       tokenName text NOT NULL,
                                       tokenSymbol text NOT NULL,
                                       tokenDecimal integer NOT NULL,
                                       transactionIndex integer NOT NULL,
                                       gas integer NOT NULL,
                                       gasPrice integer NOT NULL,
                                       gasUsed integer NOT NULL,
                                       cumulativeGasUsed integer NOT NULL,
                                       input text NOT NULL,
                                       confirmations integer NOT NULL
                                 );"""
    connection.execute(sql_create_transfers_table)

    logger.info("Getting transfers async")

    start_time = time.time()

    split_urls = [urls_transfers[i:i + 5] for i in range(0, len(urls_transfers), 5)]
    for urls in split_urls:
        asyncio.run(async_fetch_and_store(urls, connection, "Transfers", "t_transfers"))

    end_time = time.time()
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_transfers) / elapsed_time

    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_transfers"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_transfers)}")
    logger.info(f" Total Blocks: {len(urls_transfers) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")

    logger.info(" ")
    logger.info("=====================================================")
    logger.info(f"Collecting Internals transfers...")
    logger.info("=====================================================")
    logger.info("Creating Table t_internals")

    sql_create_internals_table = """CREATE TABLE IF NOT EXISTS t_internals (
                                       blockNumber integer NOT NULL,
                                       timeStamp datetime NOT NULL,
                                       hash text NOT NULL,
                                       `from` text NOT NULL,
                                       `to` text NOT NULL,
                                       value integer NOT NULL,
                                       contractAddress text NOT NULL,
                                       input text NOT NULL,
                                       type text NOT NULL,
                                       gas integer NOT NULL,
                                       gasUsed integer NOT NULL,
                                       isError integer NOT NULL,
                                       errCode text NOT NULL
                                 );"""
    connection.execute(sql_create_internals_table)

    logger.info("Getting internals async")

    start_time = time.time()

    split_urls = [urls_internals[i:i + 5] for i in range(0, len(urls_internals), 5)]
    for urls in split_urls:
        asyncio.run(async_fetch_and_store(urls, connection, "Internals", "t_internals"))

    end_time = time.time()
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_internals) / elapsed_time

    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_internals"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_internals)}")
    logger.info(f" Total Blocks: {len(urls_internals) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")

    connection.commit()

    
    logger.info("=====================================================")
    logger.info("Collecting Contract Creator data")
    logger.info("=====================================================")
    logger.info("Creating Table t_contract_creator")

    sql_create_contract_table = """CREATE TABLE IF NOT EXISTS t_contract_creator (
                                   wallet text NOT NULL,
                                   block_from integer NOT NULL,
                                   block_to text NOT NULL,
                                   first_block integer NOT NULL,
                                   first_date datetime NOT NULL,
                                   first_hash text NOT NULL,
                                   first_to text NOT NULL,
                                   first_from text NOT NULL,
                                   first_value integer NOT NULL,
                                   first_input text NOT NULL,
                                   first_func text NOT NULL,
                                   last_block text NOT NULL,
                                   last_date datetime NOT NULL,
                                   last_hash text NOT NULL,
                                   last_to text NOT NULL,
                                   last_from text NOT NULL,
                                   last_value integer NOT NULL,
                                   last_input text NOT NULL,
                                   last_func text NOT NULL
                                 );"""
    cursor.execute(sql_create_contract_table)


    contract_creator = '0xaCfE38D0C4537698783593C742803dFb47227711' #!important change to contract_creator
    base_url = f'https://rootstock.blockscout.com/api/v2/addresses/{contract_creator}/transactions'
    logger.info("Starting to retrieve transactions for the contract creator")

    # Variables to handle pagination and track the first and last transactions
    first_transaction_block = None
    last_transaction_block = None
    last_transaction = {}
    first_transaction = {}
    first_time = None
    last_time = None
    has_more_pages = True
    params = {}  # Initially, no parameters are needed for the first API request
    last_successful_params = None  # To store the last successfully fetched parameters
    fetch_count = {}  # To count fetches for each set of parameters
    creator_contracts = []  # Contract address list

    while has_more_pages:
        try:
            logger.info(f"Requesting {base_url} with params {params}")
            response = requests.get(base_url, params=params, timeout=15)  # 10-second timeout
            response.raise_for_status()  # Will raise an HTTPError for bad responses
            data = response.json()

            transactions = data['items']
            logger.debug(f"Received {len(transactions)} transactions")

            if not transactions:
                logger.info("No transactions received, ending pagination.")
                break

            current_params_signature = frozenset(params.items())
            fetch_count[current_params_signature] = fetch_count.get(current_params_signature, 0) + 1

            # Stop if the same data page is fetched more than twice
            if fetch_count[current_params_signature] > 2:
                logger.info("Repeated data fetch detected, stopping pagination.")
                break

            for transaction in transactions:
                # Check if created_contract is not null and store the address
                if transaction['created_contract']:
                    created_contract_address = transaction['created_contract']['hash']
                    creator_contracts.append(created_contract_address)
                    logger.info(f"Created contract address found: {created_contract_address}")

                if last_transaction_block is None or transaction['block'] < last_transaction_block:
                    last_transaction = transaction
                    last_transaction_block = transaction['block']
                    last_time = datetime.datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))
                if first_transaction_block is None or transaction['block'] > first_transaction_block:
                    first_transaction = transaction
                    first_transaction_block = transaction['block']
                    first_time = datetime.datetime.fromisoformat(transaction['timestamp'].replace('Z', '+00:00'))

            if 'next_page_params' in data and data['next_page_params']:
                params = data['next_page_params']
            else:
                has_more_pages = False
                logger.debug("No more pages to fetch")

        except requests.Timeout:
            logger.error("Request timed out, retrying...")
            continue

        except requests.HTTPError as http_err:
            logger.error(f"HTTP error occurred: {http_err}")
            break

        except requests.RequestException as req_err:
            logger.error(f"Request error occurred: {req_err}")
            break

        except ValueError as json_err:
            logger.error(f"JSON decoding failed: {json_err}")
            break

    # Final logs after loop
    logger.info(f"First transaction block: {first_transaction_block}, Timestamp: {first_time}")
    logger.info(f"Last transaction block: {last_transaction_block}, Timestamp: {last_time}")
    logger.info(f"Created contracts: {creator_contracts}")
    logger.info("Getting balance of contract creator")

    # Replace the web scraping with an API call
    url_balance = f'https://rootstock.blockscout.com/api/v2/addresses/{contract_creator}'
    response = requests.get(url_balance)
    balance_data = response.json()

    # Assuming 'coin_balance' is in wei, convert it to RBTC (1 RBTC = 10^18 wei)
    balance_in_wei = int(balance_data['coin_balance'])
    balance_in_rbtc = balance_in_wei / 1e18  # Convert wei to RBTC

    balance_display = f"{balance_in_rbtc:.8f} RBTC"  # Format for display with RBTC suffix
    logger.info(f"Balance of contract creator: {balance_display}")

    # Example of how you might store or handle the balance further
    # Here just logging, adjust based on your application needs
    logger.info(f"Storing balance of contract creator: {balance_display}")

    # Balances
    balances = []

    balances.append(f"Balance : {balance_in_rbtc:.8f} rBTC")

    url = f'https://rootstock.blockscout.com/api/v2/addresses/{contract_creator}/token-balances'
    response = requests.get(url)
    token_balances = response.json()

    # Process each token and convert value from wei to ethers
    logger.info(token_balances)
    for token_info in token_balances:
        token = token_info['token']
        value_in_wei = int(token_info['value'])
        value_in_eth = value_in_wei / 1e18  # Convert from wei to ethers
        token_display = f"{value_in_eth:.8f} {token['symbol']}"
        balances.append(token_display)


    last_time = datetime.datetime.fromisoformat(last_transaction['timestamp'].replace('Z', '+00:00'))

    logger.info("Storing first and last block info of contract creator")
    logger.info(first_transaction)
    cursor.execute("""INSERT INTO t_contract_creator VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", 
        (contract_creator,
         block_from,
         block_to,
         first_transaction_block,
         first_time,
         first_transaction['hash'],
         first_transaction['to']['hash'],
         first_transaction['from']['hash'],
         pyround.pyround(int(first_transaction['value']) / 1e+18, 8),
         first_transaction['raw_input'], #or decoded_input
         first_transaction['tx_types'][0],
         last_transaction['block'],
         last_time,
         last_transaction['hash'],
         last_transaction['to']['hash'],
         last_transaction['from']['hash'],
         pyround.pyround(int(last_transaction['value']) / 1e+18, 8),
         last_transaction['raw_input'],
         last_transaction['tx_types'][0]))

    connection.commit()
    
    logger.info("=====================================================")
    logger.info("Collecting Balance Creator data")
    logger.info("=====================================================")
    logger.info("Creating Table t_balance")

    sql_create_balance_table = """CREATE TABLE IF NOT EXISTS t_balance (
                                   id number NOT NULL,
                                   balance text NOT NULL
                                 );"""
    cursor.execute(sql_create_balance_table)

    logger.info("Storing balances of contract creator")
    id = 0
    for balance in balances:
        # print(f"{balance} - {type(balance)}")
        id += 1
        cursor.execute("""INSERT INTO t_balance VALUES (?, ?)""", 
            (id, balance))

    connection.commit()

    # TODO: Get the tags!!! TRX : 0xe5a4b28559a45c2d003831d4ac905842cea45df394b2eb85ad235bd96e5e443f

    logger.info(" ")
    logger.info("=====================================================")
    logger.info("Collecting Contract creator transactions")
    logger.info("=====================================================")
    logger.info("Creating Table t_transactions_wallet")

    sql_create_tran_creator_table = """CREATE TABLE IF NOT EXISTS t_transactions_wallet (
                                       blockNumber integer NOT NULL,
                                       timeStamp datetime NOT NULL,
                                       hash text NOT NULL,
                                       nonce integer NOT NULL,
                                       blockHash text NOT NULL,
                                       transactionIndex integer NOT NULL,
                                       `from` text NOT NULL,
                                       `to` text NOT NULL,
                                       value integer NOT NULL,
                                       gas integer NOT NULL,
                                       gasPrice integer NOT NULL,
                                       isError integer NOT NULL,
                                       txreceipt_status integer NOT NULL,
                                       input text NOT NULL,
                                       contractAddress text NOT NULL,
                                       cumulativeGasUsed integer NOT NULL,
                                       gasUsed integer NOT NULL,
                                       confirmations integer NOT NULL,
                                       methodId text NOT NULL,
                                       functionName text NOT NULL
                                 );"""
    connection.execute(sql_create_tran_creator_table)

    logger.info("Getting transactions of contract creator async")

    start_time = time.time()

    split_urls = [urls_trx_creator[i:i + 5] for i in range(0, len(urls_trx_creator), 5)]
    for urls in split_urls:
        asyncio.run(async_fetch_and_store(urls, connection, "Transactions", "t_transactions_wallet"))

    end_time = time.time()
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_tra_creator) / elapsed_time
    
    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_transactions_wallet"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_tra_creator)}")
    logger.info(f" Total Blocks: {len(urls_tra_creator) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")
    
    logger.info(" ")
    logger.info("=====================================================")
    logger.info(f"Collecting Contract Creator transfers...")
    logger.info("=====================================================")
    logger.info("Creating Table t_tra_creator")

    sql_create_tra_creator_table = """CREATE TABLE IF NOT EXISTS t_transfers_wallet (
                                      blockNumber integer NOT NULL,
                                      timeStamp datetime NOT NULL,
                                      hash text NOT NULL,
                                      nonce integer NOT NULL,
                                      blockHash text NOT NULL,
                                      `from` text NOT NULL,
                                      contractAddress text NOT NULL,
                                      `to` text NOT NULL,
                                      value integer NOT NULL,
                                      tokenName text NOT NULL,
                                      tokenSymbol text NOT NULL,
                                      tokenDecimal integer NOT NULL,
                                      transactionIndex integer NOT NULL,
                                      gas integer NOT NULL,
                                      gasPrice integer NOT NULL,
                                      gasUsed integer NOT NULL,
                                      cumulativeGasUsed integer NOT NULL,
                                      input text NOT NULL,
                                      confirmations integer NOT NULL
                                 );"""
    connection.execute(sql_create_tra_creator_table)

    logger.info("Getting transfers contract creator async")

    start_time = time.time()

    split_urls = [urls_tra_creator[i:i + 5] for i in range(0, len(urls_tra_creator), 5)]
    for urls in split_urls:
        asyncio.run(async_fetch_and_store(urls, connection, "Transfers", "t_transfers_wallet"))

    end_time = time.time()
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_tra_creator) / elapsed_time
    
    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_transfers_wallet"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_tra_creator)}")
    logger.info(f" Total Blocks: {len(urls_tra_creator) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")

    logger.info(" ")
    logger.info("=====================================================")
    logger.info(f"Collecting Internals contract creator transfers...")
    logger.info("=====================================================")
    logger.info("Creating Table t_internals_wsllet")

    sql_create_int_creator_table = """CREATE TABLE IF NOT EXISTS t_internals_wallet (
                                      blockNumber integer NOT NULL,
                                      timeStamp datetime NOT NULL,
                                      hash text NOT NULL,
                                      `from` text NOT NULL,
                                      `to` text NOT NULL,
                                      value integer NOT NULL,
                                      contractAddress text NOT NULL,
                                      input text NOT NULL,
                                      type text NOT NULL,
                                      gas integer NOT NULL,
                                      gasUsed integer NOT NULL,
                                      isError integer NOT NULL,
                                      errCode text NOT NULL
                                 );"""
    connection.execute(sql_create_int_creator_table)

    logger.info("Getting internals contract creator async")

    start_time = time.time()

    split_urls = [urls_int_creator[i:i + 5] for i in range(0, len(urls_int_creator), 5)]
    for urls in split_urls:
        asyncio.run(async_fetch_and_store(urls, connection, "Internals", "t_internals_wallet"))

    end_time = time.time()
    elapsed_time = end_time - start_time
    requests_per_second = len(urls_int_creator) / elapsed_time
    
    # Get total transactions registered
    query = f"SELECT COUNT(*) FROM t_internals_wallet"
    cursor.execute(query)
    total = cursor.fetchone()[0]

    logger.info(f"=========================================================")
    logger.info(f" Total requests: {len(urls_int_creator)}")
    logger.info(f" Total Blocks: {len(urls_int_creator) * chunk}")
    logger.info(f" Total TRX: {total}")
    logger.info(f" Elapsed time: {elapsed_time} seconds")
    logger.info(f" Requests per second: {requests_per_second}")
    logger.info(f"=========================================================")

    connection.commit()

    # tags - Transactions - other contract created
    query = f"SELECT DISTINCT(contractAddress) from t_transactions_wallet WHERE `to` = ''"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract Created"})

    # tags - Transactions creator
    query = f"SELECT DISTINCT(`to`) from t_transactions_wallet WHERE (input != '' AND input != '0x') AND `to` != ''"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Transactions creator
    query = f"SELECT DISTINCT(contractAddress) from t_transactions_wallet"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Internals creator
    query = f"SELECT DISTINCT(`to`) from t_internals_wallet WHERE (input != '' AND input != '0x') AND `to` != ''"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Internals creator
    query = f"SELECT DISTINCT(contractAddress) from t_internals_wallet"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # TODO: Confirm which this isn't necessary
    # # tags - Transfers creator
    # query = f"SELECT DISTINCT(`to`) from t_transfers_wallet WHERE (input != '' AND input != '0x') AND `to` != ''"
    # cursor.execute(query)
    # contracts = cursor.fetchall()
    # for c in contracts:
    #     if not any(w.get("wallet") == c[0] for w in internal_tagging):
    #         internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Transfers creator
    query = f"SELECT DISTINCT(contractAddress) from t_transfers_wallet"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Transactions
    query = f"SELECT DISTINCT(contractAddress) from t_transactions"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Internals
    query = f"SELECT DISTINCT(contractAddress) from t_internals"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # tags - Transfers
    query = f"SELECT DISTINCT(contractAddress) from t_transfers"
    cursor.execute(query)
    contracts = cursor.fetchall()
    for c in contracts:
        if not any(w.get("wallet") == c[0] for w in internal_tagging):
            internal_tagging.append({"wallet": c[0], "tag": "Contract"})

    # Get another contracts created by creator
    contracts = ""

    idx = 1
    for t in internal_tagging:
        if (idx % 5 == 0):
            creator_contracts.append(contracts)
            contracts = ""
        if (t['tag'] == 'Contract'):
            idx = idx + 1
            contracts = contracts + t['wallet'] + ','
    creator_contracts.append(contracts)
    logger.info(f"Contracts: {creator_contracts}")
    for l in creator_contracts:
        url = 'https://rootstock.blockscout.com/api/v2/smart-contracts/' + l[:-1]
        response = requests.get(url)
        t = response.json()
        internal_tagging = [{**t, **{'tag': 'Contract Created'}} for t in internal_tagging]

    logger.info(f"Storing tags")
    for t in internal_tagging:
        if t['wallet'] != '':
            # Check if t['wallet'] is a dictionary and has 'hash' key
            if isinstance(t['wallet'], dict) and 'hash' in t['wallet']:
                wallet_value = str(t['wallet']['hash'])
            else:
                wallet_value = str(t['wallet'])
            
            logger.info(str(t['tag']))
            logger.info(wallet_value)
            
            cursor.execute("""INSERT INTO t_tagging VALUES (?, ?)""", 
                        (wallet_value, str(t['tag'])))


    connection.commit()
    connection.close()
    return 0


def rsk_db_process(filename):
    # Validate file
    if (not os.path.exists(filename)):
        raise FileNotFoundError("SQLite db file not found")

    # Split in temporary files
    if (not os.path.exists('./tmp')):
        os.makedirs('./tmp')

    # Open db
    connection = sqlite3.connect(filename)
    cursor = connection.cursor()

    tic = time.perf_counter()
    cursor.execute("SELECT * FROM t_contract")
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    contract = {column_names[i]: row[i] for i in range(len(column_names))}

    # TODO: Do this in collect stage
    contract['blockchain'] = 'rsk'

    with open('./tmp/contract.json', 'w') as outfile:
        json.dump(contract, outfile)
    toc = time.perf_counter()
    logger.info(f"Get contract info in {toc - tic:0.4f} seconds")

    tic = time.perf_counter()
    # NOTE: Search Fallback function
    cursor.execute("SELECT * FROM t_source_abi")
    row = cursor.fetchone()
    column_names = [description[0] for description in cursor.description]
    source_code = {column_names[i]: row[i] for i in range(len(column_names))}
    json_str_source_code = source_code['SourceCode'] 
    contract_abi = source_code['ABI']

    fallback = False
    fallback_function = ''
    fallback_code = ''
    # receive = False  # TODO: 

    fallback_pattern  = r'(fallback[\s+]?\([.*]?\)\s)'
    fallback_result = re.search(fallback_pattern, json_str_source_code)

    if (fallback_result): 
        fallback = True
        fallback_function = fallback_result.group(1)
    if (not fallback): 
        # Compatibility
        fallback_pattern  = r'(function[\s+]?\([.*]?\)\s)'
        fallback_result = re.search(fallback_pattern, json_str_source_code)
        if (fallback_result): 
            fallback = True
            fallback_function = fallback_result.group(1)
    # Code
    if (fallback):
        funct_start = json_str_source_code.index(fallback_function)
        fallback_code = json_str_source_code[funct_start - 4:]
        funct_end = fallback_code[12:].index("function ")
        fallback_code = fallback_code[:funct_end]

    source_code['fallback'] = "true" if fallback else "false"
    source_code['fallback_function'] = fallback_function
    source_code['fallback_code'] = fallback_code

    with open('./tmp/sourcecode.json', 'w') as outfile:
        json.dump(source_code, outfile)
    with open('./tmp/contract-abi.json', 'w') as outfile:
        json.dump(contract_abi, outfile)
    toc = time.perf_counter()
    logger.info(f"Get Source code and ABI info in {toc - tic:0.4f} seconds")

    # Preprocess Statistics
    tic = time.perf_counter()
    
    # NOTE: Read db to pd
    # df_transaction = pd.read_json('./tmp/transactions.json')
    # df_t = pd.read_json('./tmp/transfers.json')
    # df_i = pd.read_json('./tmp/internals.json')
    # df_l = pd.read_json('./tmp/logs.json')
    
    df_transaction = pd.read_sql_query("SELECT * FROM t_transactions ORDER BY timeStamp ASC", connection, parse_dates=['timeStamp'])
    df_t = pd.read_sql_query("SELECT * FROM t_transfers ORDER BY timeStamp ASC", connection, parse_dates=['timeStamp'])
    df_i = pd.read_sql_query("SELECT * FROM t_internals ORDER BY timeStamp ASC", connection, parse_dates=['timeStamp'])

    # Split in JSON files
    with open('./tmp/transactions.json', 'w') as outfile:
        df_transaction.to_json(outfile)
    with open('./tmp/transfers.json', 'w') as outfile:
        df_t.to_json(outfile)
    with open('./tmp/internals.json', 'w') as outfile:
        df_i.to_json(outfile)
    toc = time.perf_counter()
    logger.info(f"Split file in {toc - tic:0.4f} seconds")

    # NOTE: For DEBUG (remove)
    df_transaction.to_csv('./tmp/transaction.csv')
    df_t.to_csv('./tmp/transfers.csv')
    df_i.to_csv('./tmp/internals.csv')
    # df_l.to_csv('./tmp/logs.csv')

    native = False
    if (df_i.size > df_t.size):
        native = True
        logger.info(f"Detect NATIVE token")
    else: 
        logger.info(f"Detect NOT NATIVE token")

    # Get unified transaction-internals-transfers
    df_trx_0 = df_transaction[df_transaction['isError'] == 0]
    df_trx_1 = df_trx_0[['timeStamp', 'hash', 'from', 'to', 'value', 'input', 
                         'isError']]
    df_trx_1.insert(len(df_trx_1.columns), "file", "trx")
    df_uni = df_trx_1

    if (not df_t.empty):
        df_tra_0 = df_t
        df_tra_1 = df_tra_0[['timeStamp', 'hash', 'from', 'to', 'value', 'input']]
        df_tra_1.insert(len(df_tra_1.columns), "isError", 0)
        df_tra_1.insert(len(df_tra_1.columns), "file", "tra")

    if (not df_i.empty):
        df_int_0 = df_i
        df_int_1 = df_int_0[['timeStamp', 'hash', 'from', 'to', 'value', 'input',
                             'isError']]
        df_int_1.insert(len(df_int_1.columns), "file", "int")

    if (not df_t.empty):
        pd_uni = pd.concat([df_uni, df_tra_1], axis=0, ignore_index=True)
        df_uni = pd.DataFrame(pd_uni)
    if (not df_i.empty):
        pd_uni = pd.concat([df_uni, df_int_1], axis=0, ignore_index=True)
        df_uni = pd.DataFrame(pd_uni)

    df_uni = df_uni.sort_values(by=['timeStamp','file'], ascending=False)  
    df_uni.to_csv('./tmp/uni.csv')
    with open('./tmp/uni.json', 'w') as outfile:
        df_uni_json = df_uni.to_json(outfile)
    
    # Get contract creator
    contract_creator = df_transaction["from"][0] #!fix

    # TODO : Determine decimal digits in base of range 

    # Get token and volume NOTE: Remove death code
    if (native):
        token_name = "rBTC"
        # volume = round(df_transaction['value'].sum() / 1e+18, 2) + round(df_i['value'].sum() / 1e+18, 2)
        volume = round((df_transaction['value'].sum() / 1e+18, 2 + df_i['value'].sum() / 1e+18) / 2, 2)
    else:
        # NOTE : Display another tokens
        token = df_t.groupby('tokenSymbol').agg({'value': ['sum','count']})
        token = token.sort_values(by=[('value','count')], ascending=False)  
        token_name = token.index[0]
        # volume = round(token.iloc[0,0] / 1e+18, 2)
        volume = round(float(token.iloc[0,0] / 1e+18) / 2, 2)

    # Liquidity and dates
    address_contract = contract["contract"].lower()
    liq = 0
    liq_raw = 0
    max_liq = 0
    max_liq_raw = 0
    volume_raw = 0
    trx_out = 0 
    trx_in = 0
    trx_out_day = 0 
    trx_in_day = 0
    remain = 0
    liq_series = []
    trx_in_series = []
    trx_out_series = []
    day = ''
    day_prev = ''
    day_prev_complete = ''
    if (native):
        # NOTE : I replace the dftemp for df_uni. Remove if it's working
        dftemp = df_uni
        dftemp = dftemp.sort_values(["timeStamp"])

        unique_wallets = len(dftemp['from'].unique())

        for i in dftemp.index: 
            value_raw = dftemp["value"][i]
            value = round(value_raw / 1e+18, 2)
            address_from = dftemp["from"][i]
            address_to = dftemp["to"][i]

            if (address_from == address_contract):
                liq = liq - value
                liq_raw = liq_raw - int(value_raw)
                trx_out = trx_out + value
                trx_out_day = trx_out_day + value
            elif (address_to == address_contract):
                liq = liq + value
                liq_raw = liq_raw + int(value_raw)
                volume_raw = volume_raw + int(value_raw)
                trx_in = trx_in + value
                trx_in_day = trx_in_day + value
                # WARNING : Remove max_liq
                # if (max_liq < liq):
                #     max_liq = liq
                #     max_liq_date = dftemp["timeStamp"][i]
                if (max_liq_raw < liq_raw):
                    max_liq_raw = liq_raw
                    max_liq_date = dftemp["timeStamp"][i]
            else:
                remain = remain + value

            if (day == ''):
                day = dftemp['timeStamp'][i].strftime("%Y-%m-%d")
                day_prev = day
                day_prev_complete = dftemp['timeStamp'][i]
            elif (day_prev != day):
                liq_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                "value": liq})
                trx_in_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                      "value": trx_in_day})
                trx_out_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                       "value": trx_out_day})
                day_prev = day
                day_prev_complete = dftemp['timeStamp'][i]
                trx_in_day = 0
                trx_out_day = 0
            else:
                day = dftemp['timeStamp'][i].strftime("%Y-%m-%d")

        first_date = dftemp['timeStamp'][0]
        last_date = dftemp['timeStamp'].iloc[-1]  # TODO: When Liq == 0

    else:  # NOTE : Not native
        unique_wallets = len(df_t['from'].unique())
        for i in df_t.index: 
            if (df_t["tokenSymbol"][i] == token_name):
                value_raw = df_t["value"][i]
                value = round(value_raw / 1e+18, 2)
                address_from = df_t["from"][i].lower()
                address_to = df_t["to"][i].lower()

                if (address_from == address_contract):
                    liq = liq - value
                    liq_raw = liq_raw - int(value_raw)
                    trx_out = trx_out + value
                    trx_out_day = trx_out_day + value
                elif (address_to == address_contract):
                    liq = liq + value
                    liq_raw = liq_raw + int(value_raw)
                    volume_raw = volume_raw + int(value_raw)
                    trx_in = trx_in + value
                    trx_in_day = trx_in_day + value
                    if (max_liq_raw < liq_raw):
                        max_liq_raw = liq_raw
                        max_liq_date = df_t["timeStamp"][i]
                else:
                    remain = remain + value

                if (day == ''):
                    day = df_t['timeStamp'][i].strftime("%Y-%m-%d")
                    day_prev = day
                    day_prev_complete = df_t['timeStamp'][i]
                elif (day_prev != day):
                    liq_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                       "value": liq})
                    trx_in_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                          "value": trx_in_day})
                    trx_out_series.append({"name": day_prev_complete.strftime("%Y-%m-%dT%H:%M:%S.009Z"),
                                           "value": trx_out_day})
                    day_prev = day
                    day_prev_complete = df_t['timeStamp'][i]
                    trx_in_day = 0
                    trx_out_day = 0
                else:
                    day = df_t['timeStamp'][i].strftime("%Y-%m-%d")

        first_date = df_t['timeStamp'][0]
        last_date = df_t['timeStamp'].iloc[-1]  # TODO : When Liq == 0

    ## Statistics
    # Unification for Native
    if (native):
        df_t = dftemp
    # Group by
    from_trx = df_t.groupby('from').agg({'value': ['sum','count']})
    # from_trx.set_axis(['value_out', 'count_out'], axis=1, inplace=False)
    from_trx_axis = from_trx.set_axis(['value_out', 'count_out'], axis=1)
    to_trx = df_t.groupby('to').agg({'value': ['sum','count']})
    # to_trx.set_axis(['value_in', 'count_in'], axis=1, inplace=True)
    to_trx_axis = to_trx.set_axis(['value_in', 'count_in'], axis=1)

    # Merge
    trx_total = from_trx_axis.join(to_trx_axis, how='outer')  # NOTE: OUTER
    trx_total['wallet'] = trx_total.index
    trx_total = trx_total.sort_values(["wallet"])
    trx_total.reset_index(drop=True, inplace=True)
    trx_total.fillna(0, inplace=True)

    total = []
    # JSON Bubbles file
    for i in trx_total.index: 
        wallet = trx_total["wallet"][i]

        child = [{"name": "IN", 
                  "size": round(trx_total["value_in"][i] / 1e+18, 2),
                  "count": int(trx_total["count_in"][i])}]
        child.append({"name": "OUT", 
                      "size": round(trx_total["value_out"][i] / 1e+18, 2),
                      "count": int(trx_total["count_out"][i])})
        item = {"name": wallet, "children": child}
        total.append(item)

    json_bubbles = {"name": "Schema", "children": total}

    with open('./tmp/bubbles.json', 'w') as outfile:
        json.dump(json_bubbles, outfile)

    # Add Percentage
    trx_total["Percentage"] = round(trx_total["value_in"] * 100 / trx_total['value_out'], 0)
    trx_total.replace([np.inf, -np.inf], 9999999, inplace=True)  # NOTE: Handle division by 0
    trx_total_dec = trx_total.sort_values(["Percentage"])
    trx_total_asc = trx_total.sort_values(["Percentage"], ascending=False)

    # Anomalies
    df_anomalies = trx_total_asc[trx_total_asc['wallet'] != address_contract ]
    df_anomalies = df_anomalies[trx_total_asc['Percentage'] >= 200]
    df_anomalies['Percentage'] = df_anomalies['Percentage'].astype(int)
    with open('./tmp/anomalies.json', 'w') as outfile:
        df_anomalies_json = df_anomalies.to_json(outfile, orient="records")

    # Statistic Percentage
    # TODO: Define the correct percentage in base to time period
    e_0 = trx_total[trx_total['Percentage'] == 0]
    e_0_100 = trx_total[(trx_total['Percentage'] > 0) & (trx_total['Percentage'] < 100)]
    e_100_241 = trx_total[(trx_total['Percentage'] >= 100) & (trx_total['Percentage'] <= 241)]
    e_241 = trx_total[trx_total['Percentage'] > 241]

    investments = []
    investments.append({"name": "Total Loses", "value": len(e_0)})
    investments.append({"name": "Loses", "value": len(e_0_100)})
    investments.append({"name": "Earnings", "value": len(e_100_241)})
    investments.append({"name": "Top Profit", "value": len(e_241)})

    # ABI
    ABI = json.loads(contract_abi)

    df_hash = df_transaction["hash"][0:]
    input_constructor = df_transaction["input"][0]
    input_column = df_transaction["input"][1:]

    decoder = InputDecoder(ABI)
    constructor_call = decoder.decode_constructor((input_constructor),)

    functions = []
    functions.append(constructor_call.name)
    arguments = []
    arguments.append(str(constructor_call.arguments))
    for i in input_column:
        try:
            func_call = decoder.decode_function((i),)
            functions.append(func_call.name)
            arguments.append(str(func_call.arguments))
        except:
            # HACK: Fallback
            if (source_code['fallback'] == "true"):
                functions.append('fallback')
                arguments.append("null")
            else:
                functions.append("Not decoded")
                arguments.append("Not decoded")

    df_decoded = pd.DataFrame({"hash": df_hash, "funct": functions, "args": arguments})
    # df_decoded = pd.DataFrame({"hash": df_hash, "funct": functions})
    dict_decoded = df_decoded.to_dict()

    with open('./tmp/decoded.json', 'w') as outfile:
        json.dump(dict_decoded, outfile)

    # Functions stats
    funct_stats = df_decoded['funct'].value_counts()
    funct_stats_json = []
    for i in funct_stats.index: 
        funct_stats_json.append({"name": str(i), "value": int(funct_stats[i])})

    json_stats = {"contract": address_contract,
                  "fdate": first_date.strftime("%Y/%m/%d - %H:%M:%S"),
                  "ldate": last_date.strftime("%Y/%m/%d - %H:%M:%S"),
                  "token_name": token_name,
                  "native": int(native),
                  "creator": contract_creator,
                  "max_liq": round(max_liq_raw / 1e18, 2),
                  "max_liq_date": max_liq_date.strftime("%Y/%m/%d - %H:%M:%S"),
                  "volume": round(volume_raw / 1e18, 2),
                  "wallets": unique_wallets,
                  "investments": investments,
                  "funct_stats": funct_stats_json,
                #   "funct_stats": investments,
                  "trx_out": trx_out,
                  "trx_in": trx_in}

    with open('./tmp/stats.json', 'w') as outfile:
        json.dump(json_stats, outfile)

    # Liquidity graph
    json_liq = liq_series

    with open('./tmp/liq.json', 'w') as outfile:
        json.dump(json_liq, outfile)

    # Trans Volume graph
    json_trans_vol = [{"name": "Trans OUT",
                      "series": trx_out_series},
                      {"name": "Trans IN",
                      "series": trx_in_series}]

    with open('./tmp/trans_series.json', 'w') as outfile:
        json.dump(json_trans_vol, outfile)

    # Transaction resume
    trans_ok = len(df_transaction[df_transaction['isError'] == 0])
    trans_error = len(df_transaction[df_transaction['isError'] != 0])

    trans_resume = [{'name': 'Transactions OK', 'value': trans_ok}, 
                    {'name': 'Transactions ERROR', 'value': trans_error}]

    with open('./tmp/transaction_resume.json', 'w') as outfile:
        json.dump(trans_resume, outfile)

    # Transfer resume
    if (native):  # NOTE: For native
        trans_in = len(dftemp_transaction)
        trans_out = len(dftemp_i)
    else:
        trans_in = len(df_t[df_t['from'].str.contains(address_contract, case=False)])
        trans_out = len(df_t[df_t['to'].str.contains(address_contract, case=False)])

    trans_io = [{'name': 'Transfers IN', 'value': trans_in}, 
                {'name': 'Transfers OUT', 'value': trans_out}]

    with open('./tmp/transfer_resume.json', 'w') as outfile:
        json.dump(trans_io, outfile)

    toc = time.perf_counter()
    logger.info(f"Preprocess Statistic file in {toc - tic:0.4f} seconds")

    return 0
