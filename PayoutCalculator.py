from collections import defaultdict
import requests
import sys
from pymongo import MongoClient, ASCENDING, DESCENDING

balance_depth = 1000
reward_coef = 0.85

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
    Okay, well. Python, khem.
    1. Input data: miner's address
    2. Skip blocks until we find the first one made by given miner, that's when we start the count
    3. In every block look for Lease (type 8) txs towards miner (miner should be recipient)
        Save: sender, amount, leaseId, height
    4. In every block look for LeaseCancel (type 9) txs for LeaseTxs (by leaseId)
        Save: sender, leaseId, height
    5. For every block:
        * save it's fee in case next one would be miner's block
        * 
"""

actual_start_height = 715315
step = 100


def get_blocks(node_api, from_block, to_block):
    return requests.get(node_api + '/blocks/seq/{}/{}'.format(from_block, to_block)).json()


def find_leases(txs, height, miner):
    # for lease in ():
    return [LeaseTx(tx['sender'], tx['amount'], tx['id'], height) for tx in txs if(tx['type'] == 8 and tx['recipient'] == miner)]


def find_canceled_leases(txs, known_leases, height):
    return [CanceledLease(tx['leaseId'], height) for tx in txs if(tx['type'] == 9 and tx['leaseId'] in known_leases)]


def total_stake_at_height(profiles, height):
    return sum(map(lambda p: p.stake_for_height(height), profiles))


if __name__ == '__main__':
    print('Payout calculator launched')
    node_api = sys.argv[1]
    miner = sys.argv[2]
    db_client = MongoClient(port=27017)
    db = db_client.payout_calculator
    print('What do you want to do?', 'Type one of the following commands:', 'crawl', 'calculate', sep='\n')
    command = input("On your command: ")
    if command == 'crawl':
        print('Starting the crawler')
        blockchain_height = requests.get('{}/blocks/height'.format(node_api)).json()['height']
        for seq_start in range(actual_start_height, blockchain_height, step):
            seq_end = seq_start + step - 1
            print('Crawling blocks from {} to {}'.format(seq_start, seq_end))
            blocks = get_blocks(node_api, seq_start, seq_end)
            db.blocks.insert_many(blocks)
        print('Crawling done')
        sys.exit()

    elif command == 'calculate':
        print('Launching calculation')
        known_leases = []
        known_lease_ids = []
        cancelled_leases = []
        mined_blocks = []
        for block in db.blocks.find().sort([('height', ASCENDING)]):
            txs = block['transactions']
            block_height = block['height']
            leases = find_leases(txs, block_height, miner)
            if len(leases) > 0:
                known_leases.extend(leases)
                known_lease_ids.extend(list(map(lambda l: l.lease_id, leases)))
                print('Found leases:', *leases, sep='\n')

            cancelled = find_canceled_leases(txs, known_lease_ids, block_height)
            if len(cancelled) > 0:
                cancelled_leases.extend(cancelled)
                print('Found cancelled leases:', *cancelled, sep='\n')

            if block['generator'] == miner:
                block_fee = block['fee']
                parent_block_id = block['reference']
                previous_block_fee = db.blocks.find_one({'signature': parent_block_id})['fee']
                earned_fee = int(0.4 * block_fee + 0.6 * previous_block_fee)
                if earned_fee > 0:
                    mined_blocks.append(MinedBlock(earned_fee, block_height))

        print('Found leases:', *known_leases, sep='\n')
        print('Cancelled leases:', *cancelled_leases, sep='\n')
        print('Mined blocks with non-zero fees:', *mined_blocks, sep='\n')
        print('Total earned fees: ', sum(map(lambda b: b.calculated_fees, mined_blocks)))

        senders = set(map(lambda l: l.sender, known_leases))
        print('{} unique leasers'.format(len(senders)))

        leaser_profiles = []
        for sender in senders:
            all_leases = [lease for lease in known_leases if lease.sender == sender]
            all_leases_ids = list(map(lambda x: x.lease_id, all_leases))
            all_canceled = [cancelled for cancelled in cancelled_leases if cancelled.lease_id in all_leases_ids]
            profile = LeaserProfile(sender, all_leases, all_canceled)
            leaser_profiles.append(profile)
            print('Constructed a profile: {}'.format(profile))

        print('Constructed leaser profiles:', *leaser_profiles, sep='\n')
        max_height_in_db = db.blocks.find().sort([('height', DESCENDING)]).limit(1).next()['height']
        stake_at_last_height = sum(map(lambda p: p.stake_for_height(max_height_in_db), leaser_profiles))
        print('Check the stake at last crawled height {}: {}'.format(max_height_in_db, stake_at_last_height))

        payouts = defaultdict(lambda: 0)
        for block in mined_blocks:
            height = block.height
            total_stake = total_stake_at_height(leaser_profiles, height)
            print('For block mined at height {} with total stake {}'.format(height, total_stake))
            for leaser in leaser_profiles:
                leased_at_height = leaser.stake_for_height(height)
                if leased_at_height > 0:
                    reward_for_height = round(reward_coef * block.calculated_fees * (leased_at_height / total_stake))
                    print("\tLeaser '{}' gets {} reward".format(leaser.sender, reward_for_height))
                    payouts[leaser.sender] += reward_for_height

        print('Here are the overall payouts for height {}'.format(max_height_in_db))
        for leaser_address, payout_amount in payouts.items():
            print("Leaser '{}' -> {}".format(leaser_address, payout_amount))
        print('Total payouts: {}'.format(sum(payouts.values())))

        left_leasers = [x.sender for x in leaser_profiles if x.stake_for_height(max_height_in_db) == 0]
        print('Left leasers: ', *left_leasers, sep='\n')
        print('Calculation done')
        sys.exit()

    else:
        raise Exception('Unexpected command: {}'.format(command))
