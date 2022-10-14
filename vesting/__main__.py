import asyncio
import json
import os
from pydoc import text
import sys
import time
from urllib.parse import quote
from ergo_python_appkit.appkit import ErgoAppKit
from sigmastate.Values import ErgoTree
import logging
import requests
from kafka import KafkaConsumer, KafkaProducer
import threading

from vesting.VestingState import VestingState

levelname = logging.DEBUG
logging.basicConfig(format='{asctime}:{name:>8s}:{levelname:<8s}::{message}', style='{', level=levelname)
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)
logger = logging.getLogger('urllib3.connectionpool')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)

bearerToken = {
    'token': None,
    'expired': 0
}

producer: KafkaProducer = None

proxyAddress = "4qyzRyJzKPEYP9XSTWsXH6hzhMCH1rMx5JvcYpVwWpD79bLDex79Bj6hbLR1NnVVYFFLCGCTSGQDQTkh5dm5d2BQY5bQRpf1AycoDjAEZyQwxiLRpbwB8Kiqyi7mUvb5UcJRdtUQ6sqcDZpgyf3hBUCCy2vg8e8P3wSopzkensJ4SoG86upev1TXacBRqsm54dshaMdToMAyFBLD2DMsZPP89gEZF4UAuLbRxZDiK871fT1NVCwa7pWK29ySAipERxWwno112zQoF5a9htj37VavXkYTzcZQ24iVjrkrfxU12huR9ZPkvLHkrdu8y8WgFdFr5oKFMsm2teFCrXMx8n9MUEEymFSWhMXBvg5UAkKW5ido9Zo2BYWDj81ew5fUoWhdJGGCu33SegnLzbNiB6VaRNusiZSPwLBA2NZ5yF5UJrUnMZAqPqWZb7zZ2zL2cBwSrFJ7kxSrQeaJ1RNGcQiDyXmzDE9vpyWTbG9W1mW5KzzMD4B9FZoUcRYbmFdp31H5Ho27rTNfx64tr7Crgjm7WfWVp8zPXjxfjW6su6u2GK6cx3feavARGNjyKKrYW3H8yPFi1Y9ruwmNwTyW96Z42FE1D28VuD5C2SJYmegbg9nPKc3ByUbS5CHJQQ4DLX9DdgZvbtq44VsiR2VmbpZNrjMwEHybRcoiDeLNhoPqxinXNvFNjg9gSca1C47EiYy4S94eFqbY1rrcF84siSEUq31e9A6snNTcDEiQ3efCcEyCb1JgA5iLDU7kqoi6xxCt7TKVfA96EKSczjaqBk5jvrmAhZrDpwrKm1sSf8py21tUgbdyDoJccUdRniahbibSRc5PVpukkkKtAUXEDG91qNbbuh47QA2NjSMiqNQjYGNJTaiBBDsGbxXjwgFkJA45E9FaFzvMvGuJyKJY9Yx9e6KBoSq1ktY38WHkFe7PBLwyZxUowb4fmgexLKiUWfLNzoZhHYu8DuAkgRtVoPRQxZYrqdgkg4PwAF6AE7XHVJEUr6iQHwTWVkp9LajbPXKtFQmVpnFNowcVVrVSabX5aqAmEu1PKVKJjvLwumUwoyRi6NwMqudVKAEqP3vdtmqr1KWzs9mNqgAybP8qaUM9pif9CxTGUKPR5FEsgnJv3WwvdJwbYv2J"
contributionAddress = "3W5p3XzGKSAZJMS1zcdfBgewEwRcjGh2ZAFgDBytsmdHwaMh4nZn13vrn7gewyrHf73EPQ8wZxJjqeFPtyxPGXsKpTTSjn9ESsdEohf6MuotxALWgr6nmV1Dri17LSeQozyjsYs3bTCRukvAymTDGAK3m1GNZYivsaBxYT9tvf6CQEjiQEiGPf3Ju2zEsdUgfhxdcH9hiRpdqZjviih6d7VYqFXu662BzXWQrDsKTARCiUawMBWQgStMbFdPo8L1xdGUq8AFuCgz3p3VyGbpnAqVdWGk1BCyzgYzLKhmVyR6SNp8qeMzQLUassJgFLcnfgzFdEAFHRH679f2Aat2XhaGR3zFZEoVHX3kXQknVDeBgqFy6ERTx1R8yzNPb7s1YptUdhWQwxSnxaQkYRuLFn2Vd1jwBgMTDd4mC5bqUk2PzU3RtnYJuE3cWC1auK5qxuPXiFB2eRwNMyuHM7MSfacs6F3yXtC2PdTm2DSgUj7EzSDNfayh38J61AiTLnyY7RGGSmYH"
ergUsdOracleNFT = "011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f"

async def getConfig():
    configResult = requests.get("http://eo-api:8901/api/config")

    return configResult.json()

async def initiateFilters():
    topics = ['io.ergopad.vesting.mempoolcheck']

    with open('vesting/filters/tx_contribute.json') as f:
        contribute_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=contribute_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + contribute_filter['topics']

    with open('vesting/filters/tx_new_proxy.json') as f:
        new_proxy_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=new_proxy_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + new_proxy_filter['topics']

    with open('vesting/filters/tx_refund.json') as f:
        refund_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=refund_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + refund_filter['topics']

    with open('vesting/filters/tx_vest.json') as f:
        vest_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=vest_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + vest_filter['topics']

    with open('vesting/filters/tx_oracle.json') as f:
        oracle_filter = json.loads(f.read())
    res = requests.post('http://eo-api:8901/api/filter', json=oracle_filter)
    if res.ok:
        logging.info(res.json())
        topics = topics + oracle_filter['topics']
    
    logging.info(topics)
    
    return topics

async def currentVestingState(config) -> VestingState:
    result = VestingState()

    fetchedOracle = False
    while not fetchedOracle:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{ergUsdOracleNFT}',timeout=120)
        if res.ok:
            result.oracle = res.json()["items"][0]
            fetchedOracle = True
        else:
            logging.info("Timed out fetching oracle, trying again in 5 seconds")
            await asyncio.sleep(5)

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{proxyAddress}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addProxyBox(box)
            result.roundInfo[box["assets"][0]["tokenId"]] = getRoundInfo(config,box["assets"][0]["tokenId"])
            if len(box["assets"]) > 1:
                result.roundInfo[box["assets"][1]["tokenId"]] = getRoundInfo(config,box["assets"][1]["tokenId"])
        offset += limit

    offset = 0
    limit = 100
    moreBoxes = True
    while moreBoxes:
        res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byAddress/{contributionAddress}?offset={offset}&limit={limit}',timeout=120)
        boxes = res.json()["items"]
        moreBoxes = len(boxes) == limit
        for box in boxes:
            result.addContributionBox(box)
        offset += limit

    return result

async def makeTx(appKit: ErgoAppKit, vestingState: VestingState, config, producer: KafkaProducer):
    unsignedTx = None
    txType = ""
    try:
        (txType, unsignedTx) = vestingState.vestTX(appKit,config['REWARD_ADDRESS'])
    except Exception as e:
        logging.error(e)
    if unsignedTx is not None:
        try:
            signedTx = appKit.signTransaction(unsignedTx)
            signedTxJson = json.loads(signedTx.toJson(False))
            txInfo = {'type': txType, 'tx': signedTxJson}
            producer.send('ergo.submit_tx',value=txInfo)
            if txType.startswith('io.ergopad.vesting.refund'):
                notifyUser("io.ergopad.vesting.refund",signedTxJson,vestingState,appKit)
            vestingState.newTx(json.loads(signedTx.toJson(False)))
        except Exception as e:
            logging.error(e)
    if txType == "error":
        vestingState.mempool.validateMempool(config["ERGO_NODE"])

def getRoundInfo(config, proxyNFT):
    try:
        tkn = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/tokens/{proxyNFT}')
        return tkn.json()
    except Exception as e:
        logging.error(e)

def getBearerToken():
    urlAuth = f'{os.getenv("ERGOPAD_API")}/auth/token'
    username = os.getenv('ERGOPAD_USER')
    password = os.getenv('ERGOPAD_PASS')
    headers = {'accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
    data = f"""grant_type=&username={quote(username)}&password={quote(password)}&scope=&client_id=&client_secret="""
    
    # auth user
    res = requests.post(urlAuth, headers=headers, data=data)
    logging.debug(res.text)
    try:
        bearerToken = res.json()['access_token']
        return bearerToken
    except Exception as e:
        logging.error(e)

def notifyUser(topic,tx, vestingState: VestingState, appKit: ErgoAppKit):
    try:
        if os.getenv("ERGOPAD_USER") is None:
            return

        currentTime = time.time()
        txId = tx["id"]
        if "globalIndex" in tx:
            status = "confirmed"
        else:
            status = "submitted"
        if topic == "io.ergopad.vesting.refund":
            contributionBox = vestingState.getContributionBoxById(tx["inputs"][0]["boxId"])
            address = appKit.tree2Address(appKit.treeFromBytes(bytes.fromhex(vestingState.getRegisterHex(contributionBox,"R5"))))
            proxyNFT = vestingState.getRegisterHex(contributionBox,"R6")
            roundInfo = vestingState.roundInfo[proxyNFT]
            if tx["dataInputs"][0]["boxId"] == vestingState.oracle["boxId"]:
                if status == "confirmed":
                    additionalText = "Your refund has been confirmed. Please try contribute again"
                else:
                    additionalText = "Unfortunately the erg price has dropped too much for your contribution to be accepted and a refund is initiated"
            else:
                if status == "confirmed":
                    additionalText = "Your refund has been confirmed, you can try again for a smaller amount if any tokens are left"
                else:
                    additionalText = f"Unfortunately the {roundInfo['name']} round did not have enough tokens left so your contribution will be refunded"

        if topic == "io.ergopad.vesting.contribute":
            contributionBox = tx["outputs"][0]
            address = appKit.tree2Address(appKit.treeFromBytes(bytes.fromhex(vestingState.getRegisterHex(contributionBox,"R5"))))
            proxyNFT = vestingState.getRegisterHex(contributionBox,"R6")
            roundInfo = vestingState.roundInfo[proxyNFT]
            if status == "confirmed":
                additionalText = f"Your contribution for {roundInfo['name']} round has been confirmed. ErgoPads off-chain bots will attempt to vest your contribution"
            else:
                additionalText = f"Your contribution for {roundInfo['name']} round has been submitted to the mempool"

        if topic == "io.ergopad.vesting.vest":
            contributionBox = vestingState.getContributionBoxById(tx["inputs"][1]["boxId"])
            address = appKit.tree2Address(appKit.treeFromBytes(bytes.fromhex(vestingState.getRegisterHex(contributionBox,"R5"))))
            proxyNFT = vestingState.getRegisterHex(contributionBox,"R6")
            roundInfo = vestingState.roundInfo[proxyNFT]
            if status == "confirmed":
                additionalText = f"Your vest transaction for {roundInfo['name']} round has been confirmed. Check your wallet to verify you have received a vesting key"
            else:
                additionalText = f"Your vest transaction for {roundInfo['name']} round has been submitted to the mempool"

        context = f'Contribution to {roundInfo["name"]} round'
        
        if bearerToken["token"] is None or currentTime > bearerToken["expired"]:
            bearerToken["token"] = getBearerToken()
            bearerToken["expired"] = currentTime + 3600

        headers = {
            'accept': 'application/json', 
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {bearerToken["token"]}'
        }

        notification = {
            'transactionId': txId,
            'transactionStatus': status,
            'context': context,
            'additionalText': additionalText
        }

        res = requests.post(f'{os.getenv("ERGOPAD_API")}/notifications/{address}', headers=headers, json=notification)
        logging.info(res.request)
        logging.info(res.content)
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
            producer.send('io.ergopad.vesting.mempoolcheck',"{'dummy':1}")
        except Exception as e:
            logging.error(e)

async def main():
    config = await getConfig()
    threading.Thread(target=asyncio.run, args=(checkMempool(config),)).start()
    topics = await initiateFilters()
    appKit = ErgoAppKit(config['ERGO_NODE'],'mainnet',config['ERGO_EXPLORER'])
    vestingState = await currentVestingState(config)
    logging.info(vestingState)
    consumer = KafkaConsumer(*topics,group_id='io.ergopad.vesting',bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    producer = None
    while producer is None:
        try:
            producer = KafkaProducer(bootstrap_servers=f"{config['KAFKA_HOST']}:{config['KAFKA_PORT']}",value_serializer=lambda m: json.dumps(m).encode('utf-8'))
        except:
            await asyncio.sleep(2)
    await makeTx(appKit,vestingState,config,producer)
    for message in consumer:
        logging.info(message.topic)
        if message.topic == "io.ergopad.vesting.oracle":
            tx = message.value
            if "globalIndex" in tx:
                logging.info("oracle updated")
                vestingState.oracle = tx["outputs"][0]
                logging.info(vestingState.oracle)
        if message.topic == "io.ergopad.vesting.refund":
            tx = message.value
            notifyUser(message.topic,tx,vestingState,appKit)
            vestingState.newTx(tx)
        if message.topic == "io.ergopad.vesting.contribute":
            tx = message.value
            vestingState.newTx(tx)
            notifyUser(message.topic,tx,vestingState,appKit)
        if message.topic == "io.ergopad.vesting.new_proxy":
            tx = message.value
            vestingState.newTx(tx)
            vestingState.roundInfo[tx["outputs"][1]["assets"][0]["tokenId"]] = getRoundInfo(config,tx["outputs"][1]["assets"][0]["tokenId"])
            vestingState.roundInfo[tx["outputs"][1]["assets"][1]["tokenId"]] = getRoundInfo(config,tx["outputs"][1]["assets"][1]["tokenId"])
        if message.topic == "io.ergopad.vesting.vest":
            tx = message.value
            notifyUser(message.topic,tx,vestingState,appKit)
            vestingState.newTx(tx)
        if message.topic == "io.ergopad.vesting.mempoolcheck":
            logging.info("Checking mempool")
            vestingState.mempool.validateMempool(config['ERGO_NODE'])
        logging.info(vestingState)
        await makeTx(appKit,vestingState,config,producer)
        
asyncio.run(main())

            