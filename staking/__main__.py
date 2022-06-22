import asyncio
import json
from pydoc import text
import sys
import os
from ergo_python_appkit.appkit import ErgoAppKit
import logging
import requests
from kafka import KafkaConsumer, KafkaProducer
import threading

from staking.StakingState import StakingState

levelname = logging.DEBUG
logging.basicConfig(format='{asctime}:{name:>8s}:{levelname:<8s}::{message}', style='{', level=levelname)
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)
logger = logging.getLogger('urllib3.connectionpool')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)

stakeStateNFT = os.getenv("STAKE_STATE_NFT")
stakePoolNFT = os.getenv("STAKE_POOL_NFT")
emissionNFT = os.getenv("EMISSION_NFT")
stakeTokenID =  os.getenv("STAKE_TOKEN_ID")
stakedTokenID = os.getenv("STAKED_TOKEN_ID")
incentiveAddress = os.getenv("INCENTIVE_ADDRESS")
incentiveTree = os.getenv("INCENTIVE_TREE")

project = os.getenv("PROJECT")

producer: KafkaProducer = None

async def getConfig():
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()

async def initiateFilters():
    topics = [f'{project}.staking.emit_time',f'{project}.staking.mempoolcheck']

    with open('staking/filters/tx_stake_state.json') as f:
        stake_state_filter = json.loads(f.read())
        stake_state_filter["name"] = project + stake_state_filter["name"]
        stake_state_filter["topics"][0] = project + stake_state_filter["topics"][0]
        stake_state_filter["filterTree"]["comparisonValue"] = stakeStateNFT
    res = requests.post('http://eo-api:8901/api/filter', json=stake_state_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + stake_state_filter['topics']

    with open('staking/filters/tx_emission.json') as f:
        emission_filter = json.loads(f.read())
        emission_filter["name"] = project + emission_filter["name"]
        emission_filter["topics"][0] = project + emission_filter["topics"][0]
        emission_filter["filterTree"]["comparisonValue"] = emissionNFT
    res = requests.post('http://eo-api:8901/api/filter', json=emission_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + emission_filter['topics']

    with open('staking/filters/tx_incentive.json') as f:
        incentive_filter = json.loads(f.read())
        incentive_filter["name"] = project + incentive_filter["name"]
        incentive_filter["topics"][0] = project + incentive_filter["topics"][0]
        incentive_filter["filterTree"]["comparisonValue"] = incentiveTree
    res = requests.post('http://eo-api:8901/api/filter', json=incentive_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + incentive_filter['topics']
    
    return topics

async def currentStakingState(config) -> StakingState:
    result = StakingState()

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakeStateNFT}',timeout=120)
    if res.ok:
        result.stakeState = res.json()["items"][0]
    logging.info(result.nextCycleTime())

    await setTimeFilter(result)

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{emissionNFT}',timeout=120)
    if res.ok:
        result.emission = res.json()["items"][0]

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakePoolNFT}',timeout=120)
    if res.ok:
        result.stakePool = res.json()["items"][0]

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        success = False
        while not success:
            try:
                req = f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakeTokenID}?offset={offset}&limit={limit}'
                logging.debug(req)
                res = requests.get(req, timeout=120)
                if 'items' in res.json():
                    success = True
            except Exception as e:
                logging.error(f'currentStakingState::{e}')
                pass
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            if box["assets"][0]["tokenId"] == stakeTokenID:
                result.addStakeBox(box)
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{incentiveAddress}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addIncentiveBox(box)
        offset += limit

    return result

async def setTimeFilter(stakingState: StakingState):
    with open('staking/filters/block_emit_time.json') as f:
        block_time_filter = json.loads(f.read())
        block_time_filter["name"] = project + block_time_filter["name"]
    block_time_filter["filterTree"]["comparisonValue"] = stakingState.nextCycleTime()
    requests.post('http://eo-api:8901/api/filter', json=block_time_filter)

async def makeTx(appKit: ErgoAppKit, stakingState: StakingState, config, producer: KafkaProducer):
    unsignedTx = None
    txType = ""
    try:
        unsignedTx = stakingState.emitTransaction(appKit,config['REWARD_ADDRESS'])
        if unsignedTx is not None:
            txType = project + ".staking.emit"
            logging.info("Submitting emit tx")
    except Exception as e:
        logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.compoundTX(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = project + ".staking.compound"
                logging.info("Submitting compound tx")
        except Exception as e:
            pass#logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.consolidateTransaction(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = project + ".staking.consolidate"
                logging.info("Submitting consolidate tx")
        except Exception as e:
            pass#logging.error(e)
    if unsignedTx is not None:
        try:
            signedTx = appKit.signTransaction(unsignedTx)
            signedTxJson = json.loads(signedTx.toJson(False))
            txInfo = {'type': txType, 'tx': signedTxJson}
            producer.send('ergo.submit_tx',value=txInfo)
            stakingState.newTx(json.loads(signedTx.toJson(False)))
        except Exception as e:
            logging.error(e)

async def checkMempool(config):
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        except:
            await asyncio.sleep(2)
    while True:
        await asyncio.sleep(240)
        try:
            producer.send(project + '.staking.mempoolcheck',"{'dummy':1}")
        except Exception as e:
            logging.error(e)

async def main():
    config = await getConfig()
    threading.Thread(target=asyncio.run, args=(checkMempool(config),)).start()
    topics = await initiateFilters()
    appKit = ErgoAppKit(config['ERGO_NODE'],'mainnet',config['ERGO_EXPLORER'])
    stakingState = await currentStakingState(config)
    logging.info(stakingState)
    consumer = KafkaConsumer(*topics,group_id=project + '.staking',bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        except:
            await asyncio.sleep(2)
    await makeTx(appKit,stakingState,config,producer)
    for message in consumer:
        if message.topic == project + ".staking.mempoolcheck":
            logging.info("Checking mempool")
            stakingState.mempool.validateMempool(config['ERGO_NODE'])
        if message.topic == project + ".staking.stake_state":
            tx = message.value
            stakingState.newTx(tx)
            if "globalIndex" in tx:
                stakingState.stakeState = tx["outputs"][0]
                if tx["outputs"][1]["assets"][0]["tokenId"] == stakeTokenID:
                    logging.info("Stake transaction")
                    stakingState.addStakeBox(tx["outputs"][1])
                else:
                    if tx["outputs"][1]["assets"][0]["tokenId"] == stakePoolNFT:
                        logging.info("Emit transaction")
                        stakingState.stakePool = tx["outputs"][1]
                        stakingState.emission = tx["outputs"][2]
                        await setTimeFilter(stakingState)
                        for outp in tx["outputs"]:
                            if outp["ergoTree"] == incentiveTree:
                                stakingState.addIncentiveBox(outp)
                    else:
                        if len(tx["outputs"][2]["additionalRegisters"]) > 0:
                            logging.info("Partial Unstake transaction")
                            stakingState.addStakeBox(tx["outputs"][2])
                        else:
                            logging.info("Unstake transaction")
                            stakingState.removeStakeBox(tx["inputs"][1]["boxId"])
        if message.topic == project + ".staking.emission":
            tx = message.value
            stakingState.newTx(tx)
            if "globalIndex" in tx:
                logging.info("Compound transaction")
                stakingState.emission = tx["outputs"][0]
                for outp in tx["outputs"]:
                    if len(outp["assets"]) > 0:
                        if outp["assets"][0]["tokenId"] == stakeTokenID:
                            stakingState.addStakeBox(outp)
                    if outp["ergoTree"] == incentiveTree:
                        stakingState.addIncentiveBox(outp)
        if message.topic == project + ".staking.incentive":
            tx = message.value
            stakingState.newTx(tx)
            if "globalIndex" in tx:
                for outp in tx["outputs"]:
                    if outp["ergoTree"] == incentiveTree:
                        stakingState.addIncentiveBox(outp)
                logging.info("Funding or consolidation transaction")
        if message.topic == project + ".staking.emit_time":
            logging.info("Emission time")
        logging.info(stakingState)
        await makeTx(appKit,stakingState,config,producer)
        
asyncio.run(main())

            