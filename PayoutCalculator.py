from collections import defaultdict, namedtuple
import requests
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING
import copy
import logging

balance_depth = 1000


class LeaseTx:
    def __init__(self, sender, amount, lease_id, height):
        self.sender = sender
        self.amount = amount
        self.lease_id = lease_id
        self.height = height

    def __repr__(self):
        return "<LeaseTx leaseId:{}, sender:{}, amount:{}, height:{}>"\
            .format(self.lease_id, self.sender, self.amount, self.height)

    def __str__(self):
        return "LeaseTx leaseId: '{}', sender: '{}', amount: '{}', height: '{}'"\
            .format(self.lease_id, self.sender, self.amount, self.height)


class CanceledLease:
    def __init__(self, lease_id, height):
        self.lease_id = lease_id
        self.height = height

    def __repr__(self):
        return "<CancelLeaseTx leaseId:{}, height:{}>"\
            .format(self.lease_id, self.height)

    def __str__(self):
        return "CancelLeaseTx leaseId: '{}', height: '{}'"\
            .format(self.lease_id, self.height)


class MinedBlock:
    def __init__(self, calculated_fees, height):
        self.calculated_fees = calculated_fees
        self.height = height

    def __repr__(self):
        return "<MinedBlock at height:{}".format(self.height)

    def __str__(self):
        return "MinedBlock at height: {} with total acquired fees: {}".format(self.height, self.calculated_fees)


class LeaserProfile:
    def __init__(self, sender, leases, cancelled):
        self.sender = sender
        self.leases = leases
        self.cancelled = cancelled

    def __repr__(self):
        return "<LeaserProfile {}, leases:{}, cancelled:{}>".format(self.sender, len(self.leases), len(self.cancelled))

    def __str__(self):
        return "LeaserProfile '{}' with {} active leases".format(self.sender, len(self.leases) - len(self.cancelled))

    def is_lease_cancelled(self, lease_id, height):
        return len([x for x in self.cancelled if x.lease_id == lease_id and height >= x.height]) > 0

    def stake_for_height(self, check_height):
        stake_sum = 0
        for lease in self.leases:
            if not self.is_lease_cancelled(lease.lease_id, check_height)\
                    and lease.height + balance_depth < check_height:
                stake_sum += lease.amount
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


def find_leases(txs, height, miner):
    return [LeaseTx(tx['sender'], tx['amount'], tx['id'], height) for tx in txs if(tx['type'] == LEASE_TX_TYPE and tx['recipient'] == miner)]


def find_canceled_leases(txs, known_leases, height):
    return [CanceledLease(tx['leaseId'], height) for tx in txs if(tx['type'] == LEASE_CANCEL_TX_TYPE and tx['leaseId'] in known_leases)]


def total_stake_at_height(profiles, height):
    return sum(map(lambda p: p.stake_for_height(height), profiles))


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s %(levelname)s [%(module)s] %(message)s')
    logger = logging.getLogger('PayoutCalculator')

    logger.info('Payout calculator launched')
    arg_names = ['cmd', 'node_api', 'miner', 'maybe_command']
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

    # That's insecure, should use username & password from some file or whatever
    db_client = MongoClient(port=27017)
    db = db_client.payout_calculator

    if command == 'crawl':
        logger.info('Starting the crawler')
        crawl_start_height = 1

        check_index(db.block_headers, 'height', ASCENDING)
        check_index(db.lease_txs, 'height', ASCENDING)
        check_index(db.lease_cancel_txs, 'height', ASCENDING)

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
            db.leases.insert_many(crawled_data.lease_txs)
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
        crawled_blocks_count = db.blocks.count_documents({})
        logger.info('There are {} blocks in Mongo DB'.format(crawled_blocks_count))
        last_block = db.blocks.find().sort([('height', DESCENDING)]).limit(1).next()
        logger.info("Last block's height is {}".format(last_block['height']))
        logger.info('Establishing backwards chain check starting from {}'.format(last_block['signature']))
        while last_block['height'] > 1:
            parent_block_id = last_block['reference']
            maybe_parent_block = db.blocks.find_one({'signature': parent_block_id})
            if maybe_parent_block is None:
                raise Exception("Couldn't find block '{}' for height {}".format(parent_block_id, last_block['height']))
            else:
                logger.debug('Found previous block {} at height {}'.format(maybe_parent_block['signature'], maybe_parent_block['height']))
                last_block = maybe_parent_block
        logger.info('Block storage check success: reached genesis block')

    elif command == 'calculate':
        since_height = 1
        since_height_input = input("Calculate since (height, default is 1): ")
        if len(since_height_input) == 0:
            logger.info('Going to calculate since genesis')
        elif not since_height_input.isdigit():
            logger.error('Expected height as a number, aborting')
            sys.exit(2)
        else:
            since_height = int(since_height_input)

        period_start_height = since_height
        period_start_height_input = input("Enter start period height: ")
        if len(period_start_height_input) == 0:
            logger.info('Going to calculate payouts since {} height'.format(since_height))
        elif not period_start_height_input.isdigit():
            logger.error('Expected height as a number, aborting')
            sys.exit(2)
        else:
            period_start_height = int(period_start_height_input)

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

        logger.debug('Launching calculation since height {}'.format(since_height))
        known_leases = []
        known_lease_ids = []
        cancelled_leases = []
        mined_blocks = []
        with db_client.start_session() as db_session:
            found_blocks_cursor = db.blocks.find(
                {'$and': [
                    {'height': {'$gte': since_height}},
                    {'$or': [
                        {'transactionCount': {'$gt': 0}},
                        {'$and': [
                            {'height': {'$gte': period_start_height}},
                            {'generator': miner}
                        ]}
                    ]}
                ]},
                no_cursor_timeout=True,
                session=db_session).sort([('height', ASCENDING)])
            blocks_to_process = found_blocks_cursor.count()
            logger.info('Got {} blocks to process'.format(blocks_to_process))
            iteration_count = 1
            for block in found_blocks_cursor:
                txs = block['transactions']
                block_height = block['height']

                # Update mongo session every 100th block processed
                if iteration_count % 100 == 0:
                    logger.debug('Iteration {}/{}, processing block at height {}'
                          .format(iteration_count, blocks_to_process, block_height))
                    session_update_result = db_client.admin\
                        .command('refreshSessions', [db_session.session_id], session=db_session)

                leases = find_leases(txs, block_height, miner)
                if len(leases) > 0:
                    known_leases.extend(leases)
                    known_lease_ids.extend(list(map(lambda l: l.lease_id, leases)))
                    logger.debug('Found leases:', *leases, sep='\n')

                cancelled = find_canceled_leases(txs, known_lease_ids, block_height)
                if len(cancelled) > 0:
                    cancelled_leases.extend(cancelled)
                    logger.debug('Found cancelled leases:', *cancelled, sep='\n')

                if block['generator'] == miner and block['height'] >= period_start_height:
                    block_fee = block['fee']
                    parent_block_id = block['reference']
                    previous_block = db.blocks.find_one({'signature': parent_block_id})
                    if previous_block is None:
                        raise Exception("Could not find parent block '{}' for block '{}'".\
                                        format(parent_block_id, block['signature']))

                    previous_block_fee = previous_block['fee']
                    earned_fee = int(0.4 * block_fee + 0.6 * previous_block_fee)
                    if earned_fee > 0:
                        mined_blocks.append(MinedBlock(earned_fee, block_height))

                iteration_count += 1

        logger.debug('Found leases:', *known_leases, sep='\n')
        logger.debug('Cancelled leases:', *cancelled_leases, sep='\n')
        logger.debug('Mined blocks with non-zero fees:', *mined_blocks, sep='\n')
        logger.debug('Total earned fees: ', sum(map(lambda b: b.calculated_fees, mined_blocks)))

        senders = set(map(lambda l: l.sender, known_leases))
        logger.debug('{} unique leasers'.format(len(senders)))

        leaser_profiles = []
        for sender in senders:
            all_leases = [lease for lease in known_leases if lease.sender == sender]
            all_leases_ids = list(map(lambda x: x.lease_id, all_leases))
            all_canceled = [cancelled for cancelled in cancelled_leases if cancelled.lease_id in all_leases_ids]
            profile = LeaserProfile(sender, all_leases, all_canceled)
            leaser_profiles.append(profile)

        logger.debug('Constructed leaser profiles:', *leaser_profiles, sep='\n')
        max_height_in_db = db.blocks.find().sort([('height', DESCENDING)]).limit(1).next()['height']
        stake_at_last_height = sum(map(lambda p: p.stake_for_height(max_height_in_db), leaser_profiles))
        logger.debug('Check the stake at last crawled height {}: {}'.format(max_height_in_db, stake_at_last_height))

        payouts = defaultdict(lambda: 0)
        for block in mined_blocks:
            height = block.height
            total_stake = total_stake_at_height(leaser_profiles, height)
            logger.debug('For block mined at height {} with total stake {}'.format(height, total_stake))
            for leaser in leaser_profiles:
                leased_at_height = leaser.stake_for_height(height)
                if leased_at_height > 0:
                    reward_for_height = round(reward_coef * block.calculated_fees * (leased_at_height / total_stake))
                    logger.debug("\tLeaser '{}' gets {} reward".format(leaser.sender, reward_for_height))
                    payouts[leaser.sender] += reward_for_height

        logger.info('Here are the overall payouts for height {}'.format(max_height_in_db))
        for leaser_address, payout_amount in payouts.items():
            logger.info("Leaser '{}' -> {}".format(leaser_address, payout_amount))
        logger.info('Total payouts: {}'.format(sum(payouts.values())))

        left_leasers = [x.sender for x in leaser_profiles if x.stake_for_height(max_height_in_db) == 0]
        logger.info('Left leasers: ', *left_leasers, sep='\n')
        logger.info('Calculation done')
        sys.exit()

    else:
        raise Exception('Unexpected command: {}'.format(command))
