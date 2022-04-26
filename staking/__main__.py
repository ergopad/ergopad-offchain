import asyncio
import json
from pydoc import text
import sys
import time
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

stakeStateNFT = "05cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f3125"
stakePoolNFT = "0d01f2f0b3254b4a1731e1b61ad77641fe54de2bd68d1c6fc1ae4e7e9ddfb212"
emissionNFT = "0549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59"
stakeTokenID =  "1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee"
stakedTokenID = "d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413"
incentiveAddress = "ciYus5z3iMQUYtVHoGEN4rxgv26ddxnamW8uyLV4uJdsis4NRRJ2Jy58ja5dP8wr88Qa7q3rMZVQsnX1sru2R54bwqL3xGv52v2EezZzMLX1XDTqKdQsPEGFD3G7UptKSm7T19oqe4W56pabRFmDmpKb7gjgG6QVeVAVR6cEEPtA7TaHPbGM5Hih2kxJLAmtofFiXcCQXwWgFjkDdRK9AcsARTa2wtBF9cLoQepfgAMuuXa5Be1EfsKYUfFuFviB8tWqosLw9DJSZWM3n6bLyBKysMtngXPaSDxQn7gGCbjHeVmoDHiiqifFgNFPRUrvdA62WgbGS2xJrWAR7UFMgGc4QvfKuT1j28BsVBetttfWck1oBYuq6tCtyrxvaFWz5V7g8z6A6KX2SQMcPhvKLx9om8F6oo2r4ZegWFVSUeXsmUCt3zE5RZbDoCowtLn2SQMf9Wxw8inPJqzesGHLwJY9vchXnUdYmaJSUCuH8w9pd8n29qRSR28WCgW6Hg8Vfcf5x4ybNuowjca98zfYpLJPAfQUmEbWMQ4YAPbKQCYg6zZ8QVZduWuVVMuHVEH1Qf52yfKrqqrw7mgTNvBoxUEFyQgHXqiQpVAxppfvhJgzTA3H9Dxzz8CBqVFzCmnnXAMfafcUgVabRmvV4PjCUWM4sW19cHbTrtVJ9iYarPNwjR7C1Yd18CqpsGnvjcyo87dDBvxGxSt9"
incentiveTree = "101e04000e20c66a7fdc803a68ca52666aa40b056949dec6242deb6dd7855733b6648d7a049c0e203fb016f4eb36ff7b2226fc5ee456b5d54d05e22849ff53599215aefc87a68b350404040604020580a4e803040805809bee020402040004000e201028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee04000402010004c0ee6d04e0c65b0480897a04c0843d0402040404060400058084af5f040204c0843d04040580897a0100d803d601b2a4730000d602cbc27201d603730195ed937202730293cbc2b2a47303007203d801d604b2a5730400d19683040193c5b2a499b1a4730500c5a793c27204c2a792c1720499c1a7730693c1b2a57307007308959372027203d806d604b1a4d6059972047309d606b2a5720500d607b1b5a4d9010763d801d609db630872079591b17209730aed938cb27209730b0001730c93b2e4c672070411730d00b2e4c672010411730e00730fd6089a73109c73117207d6099a73129c73137207d19683060193c5b2a4720500c5a793c27206c2a792c1720699c1a77e72080593c1b2a57204007e99720872090593c1b2a59a72047314007e7209059372049a720773159593b1a57316d803d604b2a5731700d605c2a7d606c1a7d19683050193c27204720591c172047206907206731893c1b2a57319007e9c731ab1b5a4d901076393c2720772050593c1b2a5731b00731cd1731d"

producer: KafkaProducer = None

async def getConfig():
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()

async def initiateFilters():
    topics = ['io.ergopad.staking.emit_time','io.ergopad.staking.mempoolcheck']

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

    with open('staking/filters/tx_incentive.json') as f:
        emission_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=emission_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + emission_filter['topics']
    
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
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{stakeTokenID}?offset={offset}&limit={limit}',timeout=120)
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
    block_time_filter["filterTree"]["comparisonValue"] = stakingState.nextCycleTime()
    res = requests.post('http://eo-api:8901/api/filter', json=block_time_filter)

async def makeTx(appKit: ErgoAppKit, stakingState: StakingState, config, producer: KafkaProducer):
    unsignedTx = None
    txType = ""
    try:
        unsignedTx = stakingState.emitTransaction(appKit,config['REWARD_ADDRESS'])
        if unsignedTx is not None:
            txType = "io.ergopad.staking.emit"
            logging.info("Submitting emit tx")
    except Exception as e:
        logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.compoundTX(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = "io.ergopad.staking.compound"
                logging.info("Submitting compound tx")
        except Exception as e:
            pass#logging.error(e)
    if unsignedTx is None:
        try:
            unsignedTx = stakingState.consolidateTransaction(appKit,config['REWARD_ADDRESS'])
            if unsignedTx is not None:
                txType = "io.ergopad.staking.consolidate"
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
            producer.send('io.ergopad.staking.mempoolcheck',"{'dummy':1}")
        except Exception as e:
            logging.error(e)

async def main():
    config = await getConfig()
    threading.Thread(target=asyncio.run, args=(checkMempool(config),)).start()
    topics = await initiateFilters()
    appKit = ErgoAppKit(config['ERGO_NODE'],'mainnet',config['ERGO_EXPLORER'])
    stakingState = await currentStakingState(config)
    logging.info(stakingState)
    consumer = KafkaConsumer(*topics,group_id='io.ergopad.staking',bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        except:
            await asyncio.sleep(2)
    await makeTx(appKit,stakingState,config,producer)
    for message in consumer:
        if message.topic == "io.ergopad.staking.mempoolcheck":
            logging.info("Checking mempool")
            stakingState.mempool.validateMempool(config['ERGO_NODE'])
        if message.topic == "io.ergopad.staking.stake_state":
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
        if message.topic == "io.ergopad.staking.emission":
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
        if message.topic == "io.ergopad.staking.incentive":
            tx = message.value
            stakingState.newTx(tx)
            if "globalIndex" in tx:
                for outp in tx["outputs"]:
                    if outp["ergoTree"] == incentiveTree:
                        stakingState.addIncentiveBox(outp)
                logging.info("Funding or consolidation transaction")
        if message.topic == "io.ergopad.staking.emit_time":
            logging.info("Emission time")
        logging.info(stakingState)
        await makeTx(appKit,stakingState,config,producer)
        
asyncio.run(main())

            