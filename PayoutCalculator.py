from collections import defaultdict, namedtuple
import requests
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
import copy
import logging
from itertools import groupby

balance_depth = 1000


MinedBlock = namedtuple('MinedBlock', 'calculated_fees, height')


class Lease:
    def __init__(self, lease_id, amount, activation_height, cancel_height):
        self.lease_id = lease_id
        self.amount = amount
        self.activation_height = activation_height
        self.cancel_height = cancel_height

    def __repr__(self):
        return "<LeaseTx leaseId:{}, amount:{}, activation_height:{}, cancel_height:{}>"\
            .format(self.lease_id, self.amount, self.activation_height, self.cancel_height)

    def __str__(self):
        return "LeaseTx leaseId: '{}', amount: '{}', activationHeight: '{}', cancelHeight: '{}'"\
            .format(self.lease_id, self.amount, self.activation_height, self.cancel_height)

    def is_active(self, at_height):
        if self.cancel_height is None:
            return self.activation_height <= at_height
        else:
            return self.activation_height <= at_height < self.cancel_height


class LeaserProfile:
    def __init__(self, sender, leases):
        self.sender = sender
        self.leases = leases

    def __repr__(self):
        return "<LeaserProfile {}, leases:{}>".format(self.sender, len(self.leases))

    def __str__(self):
        return "LeaserProfile '{}' with {} leases".format(self.sender, len(self.leases))

    def stake_for_height(self, check_height):
        active_leases_for_height = filter(lambda l: l.is_active(check_height), self.leases)
        stake_sum = sum(map(lambda l: l.amount, active_leases_for_height))
        return stake_sum


"""
    A script to crawl the blockchain and calculate leaser payouts.
    Command arguments:
        * Node REST API URL, to be used for crawling;
        * Miner address as a Base58 encoded string;
        ? Command to execute. One of: ['crawl', 'check', 'calculate']
    (Arguments marked with '?' are optional)
"""


step = 100

LEASE_TX_TYPE = 8
LEASE_CANCEL_TX_TYPE = 9


def get_blocks(node_api, from_block, to_block):
    return requests.get(node_api + '/blocks/seq/{}/{}'.format(from_block, to_block)).json()


def enrich_tx_json(txs, height):
    for tx in txs:
        tx.update({'height': height})
        if tx['type'] == LEASE_TX_TYPE:
            tx.update({'activation_height': height + balance_depth})
        yield tx


CrawledData = namedtuple('CrawledData', 'block_headers, lease_txs, lease_cancel_txs')


def extract_crawled_data(blocks):
    """
        Every block is torn apart into block headers, lease txs and lease_cancel txs
        Method returns a sequence of CrawledData objects
    """
    block_headers = []
    lease_txs_accumulator = []
    lease_cancel_txs_accumulator = []
    for block in blocks:
        height = block['height']
        block_txs = copy.deepcopy(block['transactions'])
        filtered_lease_txs = filter(lambda tx: tx['type'] == LEASE_TX_TYPE, block_txs)
        lease_txs = enrich_tx_json(filtered_lease_txs, height)
        filtered_lease_cancel_txs = filter(lambda tx: tx['type'] == LEASE_CANCEL_TX_TYPE, block_txs)
        lease_cancel_txs = enrich_tx_json(filtered_lease_cancel_txs, height)
        lease_txs_accumulator.extend([x for x in lease_txs])
        lease_cancel_txs_accumulator.extend([x for x in lease_cancel_txs])
        block.pop('transactions', None)
        block_headers.append(block)
    return CrawledData(block_headers, lease_txs_accumulator, lease_cancel_txs_accumulator)


def check_index(db_collection, index_name, direction):
    if any(index_key for index_key in db_collection.index_information().keys() if index_key.startswith(index_name)):
        logger.info("Index '{}' is present for collection '{}'".format(index_name, db_collection.name))
    else:
        db_collection.create_index([(index_name, direction)])
        logger.info("Created index '{}' for collection '{}'".format(index_name, db_collection.name))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s] %(message)s')
    logger = logging.getLogger('PayoutCalculator')

    logger.info('Payout calculator launched')
    arg_names = ['cmd', 'node_api', 'miner', 'maybe_command', 'maybe_loglevel']
    named_args = dict(zip(arg_names, sys.argv))
    if named_args.get('node_api') is None:
        logger.error('Node api not specified')
        sys.exit(2)
    node_api = named_args.get('node_api')

    if named_args.get('miner') is None:
        logger.error('Miner is not defined')
        sys.exit(2)
    miner = named_args.get('miner')

    if named_args.get('maybe_command') is None:
        logger.info('What do you want to do?', 'Type one of the following commands:', 'crawl', 'check', 'calculate', sep='\n')
        command = input("On your command: ")
    else:
        command = named_args.get('maybe_command')

    if not named_args.get('maybe_loglevel') is None:
        logger.setLevel(named_args.get('maybe_loglevel'))

    # That's insecure, should use username & password from some file or whatever
    db_client = MongoClient(port=27017)
    db = db_client.payout_calculator

    if command == 'crawl':
        logger.info('Starting the crawler')
        crawl_start_height = 1

        check_index(db.block_headers, 'height', ASCENDING)
        check_index(db.block_headers, 'signature', ASCENDING)
        check_index(db.leases, 'activation_height', ASCENDING)
        check_index(db.leases, 'id', ASCENDING)

        if db.block_headers.count_documents({}) != 0:
            crawl_start_height = db.block_headers.find().sort([('height', DESCENDING)]).next()['height'] + 1

        blockchain_height = requests.get('{}/blocks/height'.format(node_api)).json()['height']
        logger.info("Initiating crawling process from '{}' to '{}'".format(crawl_start_height, blockchain_height))

        for seq_start in range(crawl_start_height, blockchain_height, step):
            seq_end = seq_start + step - 1
            logger.debug('Crawling blocks from {} to {} ({} left)'.format(seq_start, seq_end, blockchain_height - seq_start))
            blocks = get_blocks(node_api, seq_start, seq_end)
            crawled_data = extract_crawled_data(blocks)

            db.block_headers.insert_many(crawled_data.block_headers)
            if len(crawled_data.lease_txs) > 0:
                db.leases.insert_many(crawled_data.lease_txs)

            if len(crawled_data.lease_cancel_txs) > 0:
                db.lease_cancel_txs.insert_many(crawled_data.lease_cancel_txs)
                # Process lease cancels one by one, updating leases with 'cancel_height' and 'lease_cancel_id'
                for lease_cancel in crawled_data.lease_cancel_txs:
                    lease_id = lease_cancel['leaseId']
                    logger.debug("Updating cancelled lease with id '{}'".format(lease_id))
                    db.leases.update_one({"id": lease_id},
                                         {"$set": {
                                             "cancel_height": lease_cancel['height'],
                                             "lease_cancel_id": lease_cancel['id']
                                         }})

        logger.info('Crawling done')
        sys.exit()

    elif command == 'check':
        logger.error("Checking isn't implemented for newer DB structure")
        sys.exit(2)

    elif command == 'calculate':
        period_start_height = 1
        period_start_height_input = input("Enter start period height: ")
        if len(period_start_height_input) == 0:
            logger.error('Expected start period height')
            sys.exit(2)
        elif not period_start_height_input.isdigit():
            logger.error('Expected height as a number, aborting')
            sys.exit(2)
        else:
            period_start_height = int(period_start_height_input)

        period_end_height = db.block_headers.find().sort([('height', DESCENDING)]).limit(1).next()['height']

        reward_coef_input = input("Enter reward coefficient (default is 0.9): ")
        reward_coef = 0.9
        if len(reward_coef_input) == 0:
            logger.info('Going to calculate using reward coefficient of 0.9 (90% earnings for leasers)')
        else:
            try:
                reward_coef = float(reward_coef_input)
            except ValueError:
                logger.error('Expected reward_coef as a float number')
                sys.exit(2)

        possibly_active_leases = db.leases.find(
            {'$and': [
                {'recipient': miner},
                {'activation_height': {'$lt': period_end_height}},
                {'$or': [
                    {'cancel_height': {'$exists': False}},
                    {'cancel_height': {'$gt': period_start_height}}
                ]}
            ]}
        )
        active_leases = filter(lambda tx: tx['activation_height'] < tx['cancel_height'], possibly_active_leases)

        blocks_by_miner = db.block_headers.find(
            {'$and': [
                {'height': {'$gte': period_start_height}},
                {'generator': miner}
            ]}
        )

        mined_blocks = []
        total_earned_fees = 0

        for block in blocks_by_miner:
            block_fee = block['fee']
            parent_block_id = block['reference']
            previous_block = db.block_headers.find_one({'signature': parent_block_id})
            if previous_block is None:
                raise Exception("Could not find parent block '{}' for block '{}'". \
                                format(parent_block_id, block['signature']))

            previous_block_fee = previous_block['fee']
            earned_fee = int(0.4 * block_fee + 0.6 * previous_block_fee)
            total_earned_fees += earned_fee
            mined_blocks.append(MinedBlock(earned_fee, block['height']))

        logger.debug("Miner '{}' has mined {} blocks during the period".format(miner, len(mined_blocks)))
        logger.debug("Total earned fees: {}".format(total_earned_fees))

        leaser_profiles = []
        for leaser_address, leaser_lease_txs in groupby(active_leases, lambda l: l['sender']):
            leaser_leases = [
                Lease(
                    ltx['id'],
                    ltx['amount'],
                    ltx['activation_height'],
                    ltx['cancel_height']) for ltx in leaser_lease_txs]
            leaser_profiles.append(LeaserProfile(leaser_address, leaser_leases))

        logger.debug('Constructed leaser profiles:', *leaser_profiles, sep='\n')
        stake_at_last_height = sum(map(lambda p: p.stake_for_height(period_end_height), leaser_profiles))
        logger.debug('Check the stake at last crawled height {}: {}'.format(period_end_height, stake_at_last_height))

        payouts = defaultdict(lambda: 0)
        for block in mined_blocks:
            height = block.height
            total_stake = sum(map(lambda p: p.stake_for_height(height), leaser_profiles))
            logger.debug('For block mined at height {} with total stake {}'.format(height, total_stake))
            for leaser in leaser_profiles:
                leased_at_height = leaser.stake_for_height(height)
                if leased_at_height > 0:
                    reward_for_height = round(reward_coef * block.calculated_fees * (leased_at_height / total_stake))
                    logger.debug("\tLeaser '{}' gets {} reward".format(leaser.sender, reward_for_height))
                    payouts[leaser.sender] += reward_for_height

        logger.info('Here are the overall payouts for height {}'.format(period_end_height))
        for leaser_address, payout_amount in payouts.items():
            logger.info("Leaser '{}' -> {}".format(leaser_address, payout_amount))
        logger.info('Total payouts: {}'.format(sum(payouts.values())))

        logger.info('Calculation done')
        sys.exit()

    else:
        raise Exception('Unexpected command: {}'.format(command))
