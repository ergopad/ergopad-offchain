import json
import sys
from ergo_python_appkit.appkit import ErgoAppKit
import logging
import requests
from kafka import KafkaConsumer

from staking.StakingState import StakingState

levelname = logging.DEBUG
logging.basicConfig(format='{asctime}:{name:>8s}:{levelname:<8s}::{message}', style='{', level=levelname)
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)

stakeStateNFT = "05cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f3125"
stakePoolNFT = "0d01f2f0b3254b4a1731e1b61ad77641fe54de2bd68d1c6fc1ae4e7e9ddfb212"
emissionNFT = "0549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59"
stakeTokenID =  "1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee"
stakedTokenID = "d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413"

def getConfig():
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()

def initiateFilters():
    topics = []

    with open('staking/filters/tx_stake_state.json') as f:
        stake_state_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=stake_state_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + stake_state_filter['topics']

    with open('staking/filters/tx_emission.json') as f:
        emission_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=emission_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + emission_filter['topics']
    
    return topics

def currentStakingState(config) -> StakingState:
    result = StakingState()

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakeStateNFT}')
    if res.ok:
        result.stakeState = res.json()["items"][0]

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{emissionNFT}')
    if res.ok:
        result.emission = res.json()["items"][0]

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakePoolNFT}')
    if res.ok:
        result.stakePool = res.json()["items"][0]

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakeTokenID}?offset={offset}&limit={limit}')
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            if box["assets"][0]["tokenId"] == stakeTokenID:
                result.addStakeBox(box)
        offset += limit

    return result

if __name__=="__main__":
    config = getConfig()
    topics = initiateFilters()
    stakingState = currentStakingState(config)
    logging.info(stakingState)
    consumer = KafkaConsumer(*topics,group_id='io.ergopad.staking',bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    for message in consumer:
        if message.topic == "io.ergopad.staking.stake_state":
            tx = message.value
            for inp in tx["inputs"]:
                stakingState.spendBox(inp["boxId"])
            stakingState.stakeState = tx["outputs"][0]
            if tx["outputs"][1]["assets"][0]["tokenId"] == stakeTokenID:
                logging.info("Stake transaction")
                stakingState.addStakeBox(tx["outputs"][1])
            else:
                if tx["outputs"][1]["assets"][0]["tokenId"] == stakePoolNFT:
                    logging.info("Emit transaction")
                    stakingState.stakePool = tx["outputs"][1]
                    stakingState.emission = tx["outputs"][2]
                else:
                    if len(tx["outputs"][2]["additionalRegisters"]) > 0:
                        logging.info("Partial Unstake transaction")
                        stakingState.addStakeBox(tx["outputs"][2])
                    else:
                        logging.info("Unstake transaction")
                        if "globalIndex" in tx:
                            stakingState.removeStakeBox(tx["inputs"][1]["boxId"])
        if message.topic == "io.ergopad.staking.emission":
            logging.info("Compound transaction")
            tx = message.value
            for inp in tx["inputs"]:
                stakingState.spendBox(inp["boxId"])
            stakingState.emission = tx["outputs"][0]
            for outp in tx["outputs"]:
                if len(outp["assets"]) > 0:
                    if outp["assets"][0]["tokenId"] == stakeTokenID:
                        stakingState.addStakeBox(outp)
        logging.info(stakingState)
            