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

from vesting.VestingState import VestingState

levelname = logging.DEBUG
logging.basicConfig(format='{asctime}:{name:>8s}:{levelname:<8s}::{message}', style='{', level=levelname)
logger = logging.getLogger('kafka')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)
logger = logging.getLogger('urllib3.connectionpool')
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.WARN)


producer: KafkaProducer = None

proxyAddress = "SA7vqpDWt8BYmP6PJxRe3TgV89iu6dhKr1sk622mW8PfxZ5WrcBZ4En3LwTmKJMvb2Cfzg4HPeK6wJ61aYKnqcFwkNh1bqPkk74KhespAF4Ga7YM3TNi33H8CtDLZXC69F8EyrMcsSex5taUYad9rBZm2kXZc1aLFEh9795wBsvXzoxPpALq82bBeVBij89gkaQ6Ny4c7QfAguqRzE8BkefSh75aGmcq7ZYtcRCuy5BMzVnWJBT4mrAwbDJBYxYdaSQ8zAvA7PA65zQvEVquLqamZg1PmApFL3aqbhjoF5Ppr2NHJeBdtgb9vMc9XDZy4Aag3V6EqujRoZ29WUQRTDp32iBNhEBTdE8PdSfpw5pZMzmSCGtCQ5eh5dXC4ahG8QL2D4NvXP78Qyu2FG6xyxo823G93Q2BqpcSesQvnz3bYrthVDMdBiiLqgwfsiXoTNg4KYk3HBftEzbfPnhTXxgrm8CcRWodKhS9tiA75WotZZcTACcrRZny7ZYETfwwSYTVasg7DxnSpZN4VPDExPigm69j8D8tp3hrP9BBESLWJF6EspxfWd4Nbx14eou4dQ4TRsxJUG5noCoXCeWEB4iyzuG9RyNTGD4h3gsAnnt8bR7FV6uWopmqHZ85Nyra5Zqh1aR6VQsbStaFL3WhSMS5pFBC5mhmuF7hdu3ZmsXsNgVEinGwM9jnukKghMBRNSpQbwqi4TZjja6fy93tUS9ggUShvRV7wE7YTKxq1RnnVQZTb6CCRvmycZFUuWehqXu7XekDUkWYPGC4KaagVbJRsXmKnqt36Re6V8NgHiTr4SjGzrj6gtbah1CfAVjVVA3Ggb1LnnXs4MFZpfmLnAGSm6PCb4Y3M53KygQ4pY7opZ1yxBAhB4tnJLa4KQj9uLy1V8mrK6xm8envCM92zhtPdAaxMZZmccQvSmMitSrVgWu419pT2yFTE6QpApZqysXtry3jRsmnEwLh72FfpE3xrfz4TBjM41tZm7RAEL8CwKfzs3qNsXCqvdLiBSdr6XTo9kTW6azMsGHLigqzg34FrkUGQW3S4aSnrapt1r7HmU9pGHukuuMcvgV4KrhPwV3chVetMLFEk4SoPavKueY4kX7KcAwEFsJcLtcXZq1kKBFS7kogsPNCnmxPj"
contributionAddress = "7odsYN5KsYMF5MC4TcHHB8FVCqMsUqg3GLNJDVhQRx4ebsbixF4vQcQ42sDGMrsekZHNBMb1LzoUGh9CNtoLiANg1ZyL6kmZAaV8vt7dmnDF69sbXAfo4SPQrn1Q5NuxeBtKsasC79WRp1PPJaUs3HNvEvTv7oC2mNfQt2YsGEcQwDRd6icCR65PDBAoCeKPdzKJ6FvYytbiLfCTueNfLZzmepevp9KL4fdEnJZ3aoXsPRf4QCdUMo39tWs8Ds4rEcNhPwh2HqC3j4iNcCPkX3wLeP9GALm4ARQoNgfahGpgCk9MbXkDR4Gjka5CbJwrdLDTj3wk1bXo5Zbfm4kTSHLhsvdkvxFDvp4EXbuVDN6BPepPmRJizfo5KsWRfunaFCDokPkyk23vLYyT5ZjLZvmyHZr6JvjctZN4A6qKcp8u6MuWLhPxMdmti1AH6Qp5hFEhtmLvc7G4nRanYFgtYhENNXVyWioPxyrdUa31BBrf521FDaFC4S"
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

    res = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/boxes/unspent/byTokenId/{ergUsdOracleNFT}',timeout=120)
    result.oracle = res.json()["items"][0]

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
            vestingState.newTx(json.loads(signedTx.toJson(False)))
        except Exception as e:
            logging.error(e)

def getRoundInfo(config, proxyNFT):
    try:
        tkn = requests.get(f'{config["ERGO_EXPLORER"]}/api/v1/tokens/{proxyNFT}')
        return tkn.json()
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
            vestingState.newTx(tx)
        if message.topic == "io.ergopad.vesting.contribute":
            tx = message.value
            vestingState.newTx(tx)
        if message.topic == "io.ergopad.vesting.new_proxy":
            tx = message.value
            vestingState.newTx(tx)
            vestingState.roundInfo[tx["outputs"][1]["assets"][0]["tokenId"]] = getRoundInfo(config,tx["outputs"][1]["assets"][0]["tokenId"])
            vestingState.roundInfo[tx["outputs"][1]["assets"][1]["tokenId"]] = getRoundInfo(config,tx["outputs"][1]["assets"][1]["tokenId"])
        if message.topic == "io.ergopad.vesting.vest":
            tx = message.value
            vestingState.newTx(tx)
        if message.topic == "io.ergopad.vesting.mempoolcheck":
            logging.info("Checking mempool")
            vestingState.mempool.validateMempool(config['ERGO_NODE'])
        logging.info(vestingState)
        await makeTx(appKit,vestingState,config,producer)
        
asyncio.run(main())

            