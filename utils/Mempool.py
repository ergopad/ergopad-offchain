import json
import logging
import requests

class Mempool:
    def __init__(self) -> None:
        self._inputs = {}
        self._outputs = {}
        self._utxoTokenIdCache = {}
        self._utxosByTree = {}

    def addTx(self, tx) -> None:
        for inputs in tx["inputs"]:
            boxDict = self._inputs.get(inputs["boxId"],{})
            boxDict[tx["id"]] = inputs
            self._inputs[inputs["boxId"]] = boxDict
        for outputs in tx["outputs"]:
            boxDict = self._outputs.get(outputs["boxId"],{})
            boxDict[tx["id"]] = outputs
            self._outputs[outputs["boxId"]] = boxDict
        self._utxoTokenIdCache = {}
        self._utxosByTree = {}

    def removeTx(self, txId) -> None:
        newInputs = json.loads(json.dumps(self._inputs))
        for boxId in newInputs.keys():
            boxDict = self._inputs.get(boxId,{})
            boxDict.pop(txId,None)
            self._inputs[boxId] = boxDict
            if len(self._inputs[boxId]) <= 0:
                self._inputs.pop(boxId,None)
        newOutputs = json.loads(json.dumps(self._outputs))
        for boxId in newOutputs.keys():
            boxDict = self._outputs.get(boxId,{})
            boxDict.pop(txId,None)
            self._outputs[boxId] = boxDict
            if len(self._outputs[boxId]) <= 0:
                self._outputs.pop(boxId,None)
        self._utxoTokenIdCache = {}
        self._utxosByTree = {}

    def isSpent(self, boxId: str) -> bool:
        return boxId in self._inputs

    def getUTXOByTokenId(self, tokenId: str):
        if tokenId in self._utxoTokenIdCache:
            return self._utxoTokenIdCache[tokenId]
        for boxId in self._outputs.keys():
            if not self.isSpent(boxId):
                box = list(self._outputs[boxId].values())[0]
                for asset in box["assets"]:
                    if asset["tokenId"] == tokenId:
                        logging.info("found box in mempool")
                        self._utxoTokenIdCache[tokenId] = box
                        return box
        self._utxoTokenIdCache[tokenId] = None

    def getUTXOsByTree(self, ergoTree: str):
        if ergoTree in self._utxosByTree:
            return self._utxosByTree[ergoTree]
        result = []
        for boxId in self._outputs.keys():
            if not self.isSpent(boxId):
                box = list(self._outputs[boxId].values())[0]
                if box["ergoTree"] == ergoTree:
                    result.append(box)
        self._utxosByTree[ergoTree] = result
        return result

    def getBoxById(self, boxId):
        return list(self._outputs.get(boxId,{"0": None}).values())[0]

    def validateMempool(self, nodeUrl):
        try:
            mempoolOffset = 0
            mempoolLimit = 100
            mempoolScanDone = False
            mempoolTransactions = []
            failedTransactions = []
            while not mempoolScanDone:
                mempool_result: requests.Response = requests.get(f"{nodeUrl}/transactions/unconfirmed?offset={mempoolOffset}&limit={mempoolLimit}")
                if mempool_result.ok:
                    for tx in list(mempool_result.json()):
                        mempoolTransactions.append(tx["id"])
                if len(mempool_result.json()) < mempoolLimit:
                    mempoolScanDone = True
                mempoolOffset += mempoolLimit
            
            currentMempoolTxIds = set()
            for box in self._inputs.values():
                for key in box.keys():
                    currentMempoolTxIds.add(key)

            for txId in list(currentMempoolTxIds):
                if txId not in mempoolTransactions:
                    self.removeTx(txId)
        except Exception as e:
            logging.error(e)