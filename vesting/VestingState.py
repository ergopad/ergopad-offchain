from datetime import datetime, timedelta
from time import time
from typing import List
import logging
import os
import requests
from utils.Mempool import Mempool
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValueT
from org.ergoplatform.appkit import Address, ErgoValue, OutBox
from special.collection import CollOverArray
from sigmastate.Values import ErgoTree

contributionTree = os.getenv("CONTRIBUTION_TREE")
ergUsdOracleNFT = os.getenv("ERG_USD_ORACLE_NFT")
vestingTree = os.getenv("VESTING_TREE")
proxyTree = os.getenv("PROXY_TREE")

class VestingState:
    def __init__(self) -> None:
        self._proxyBoxes = {}
        self._contributionBoxes = {}
        self.mempool: Mempool = Mempool()
        self.oracle = None
        self.roundInfo = {}

    def getRegister(self,box,register):
        hexVal = ""
        if "serializedValue" in box["additionalRegisters"][register]:
            hexVal = box["additionalRegisters"][register]["serializedValue"]
        else:
            hexVal = box["additionalRegisters"][register]
        return ErgoValue.fromHex(hexVal)

    def getRegisterHex(self,box,register):
        hexVal = ""
        if "serializedValue" in box["additionalRegisters"][register]:
            hexVal = box["additionalRegisters"][register]["serializedValue"][4:]
        else:
            hexVal = box["additionalRegisters"][register][4:]
        return hexVal

    def addProxyBox(self, proxyBox) -> bool:
        mempool = "settlementHeight" not in proxyBox
        if not mempool:
            if proxyBox["assets"][0]["tokenId"] in self._proxyBoxes:
                if proxyBox["settlementHeight"] <= self._proxyBoxes[proxyBox["assets"][0]["tokenId"]]["settlementHeight"]:
                    return False
            self._proxyBoxes[proxyBox["assets"][0]["tokenId"]] = proxyBox
            return True
        else:
            False

    def removeProxyBox(self, proxyBoxId):
        keyToRemove = None
        for proxyBox in self._proxyBoxes.keys():
            if self._proxyBoxes[proxyBox]["boxId"] == proxyBoxId:
                keyToRemove = proxyBox
        if keyToRemove is not None:
            self._proxyBoxes.pop(keyToRemove,None)

    def getProxyBox(self, proxyNFTId):
        box = self.mempool.getUTXOByTokenId(proxyNFTId)
        if box is not None:
            return box
        return self._proxyBoxes[proxyNFTId]

    def addContributionBox(self, contributionBox) -> bool:
        mempool = "settlementHeight" not in contributionBox
        if not mempool:
            self._contributionBoxes[contributionBox["boxId"]] = contributionBox
            return True
        return False

    def removeContributionBox(self, contributionBoxId):
        self._contributionBoxes.pop(contributionBoxId,None)

    def getContributionBox(self):
        contribBoxList = list(list(self._contributionBoxes.values()) + self.mempool.getUTXOsByTree(contributionTree))
        for box in contribBoxList:
            if not self.mempool.isSpent(box["boxId"]):
                return box
    
    def getContributionBoxById(self, boxId):
        box = self.mempool.getBoxById(boxId)
        if box is not None:
            return box
        return self._contributionBoxes[boxId]

    def newTx(self, tx):
        isMempool = "globalIndex" not in tx
        if isMempool:
            self.mempool.addTx(tx)
        else:
            self.mempool.removeTx(tx["id"])
            for box in tx["inputs"]:
                self.removeContributionBox(box["boxId"])
                self.removeProxyBox(box["boxId"])
            for box in tx["outputs"]:
                if box["ergoTree"] == proxyTree:
                    self.addProxyBox(box)
                if box["ergoTree"] == contributionTree:
                    self.addContributionBox(box)

    def vestTX(self, appKit: ErgoAppKit, rewardAddress: str):
        try:
            contributionBox = self.getContributionBox()

            if contributionBox is None:
                return (None, None)

            logging.info(contributionBox)
            refundThreshold = self.getRegister(contributionBox,"R4").getValue()
            userTree = self.getRegisterHex(contributionBox,"R5")
            proxyNFT = self.getRegisterHex(contributionBox,"R6")

            contributionTokens = {}
            for asset in contributionBox["assets"]:
                contributionTokens[asset["tokenId"]] = asset["amount"]

            if len(contributionTokens) < 2 and int(self.oracle["additionalRegisters"]["R4"]["renderedValue"]) > refundThreshold:
                logging.info("refund oracle")
                inputs = appKit.getBoxesById([contributionBox["boxId"]])

                dataInputs = appKit.getBoxesById([self.oracle["boxId"]])

                refundBox = appKit.buildOutBox(
                    value = contributionBox["value"]-int(7e6),
                    tokens=contributionTokens,
                    registers=None,
                    contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(userTree)))
                )

                botOperatorBox = appKit.buildOutBox(
                    value=int(5e6),
                    tokens=None,
                    registers=None,
                    contract=appKit.contractFromAddress(rewardAddress)
                )

                unsignedTx = appKit.buildUnsignedTransaction(
                    inputs=inputs,
                    outputs=[refundBox,botOperatorBox],
                    dataInputs=dataInputs,
                    fee=int(2e6),
                    sendChangeTo=Address.create(rewardAddress).getErgoAddress())

                return ('io.ergopad.vesting.refund_oracle', unsignedTx)

            if len(self.getProxyBox(proxyNFT)["assets"]) == 1 or self.getProxyBox(proxyNFT)["assets"][1]["amount"] < contributionBox["assets"][0]["amount"]:
                logging.info("refund sold out")
                inputs = appKit.getBoxesById([contributionBox["boxId"]])

                dataInputs = appKit.getBoxesById([self.getProxyBox(proxyNFT)["boxId"]])

                refundBox = appKit.buildOutBox(
                    value = contributionBox["value"]-int(7e6),
                    tokens=contributionTokens,
                    registers=None,
                    contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(userTree)))
                )

                botOperatorBox = appKit.buildOutBox(
                    value=int(5e6),
                    tokens=None,
                    registers=None,
                    contract=appKit.contractFromAddress(rewardAddress)
                )

                unsignedTx = appKit.buildUnsignedTransaction(
                    inputs=inputs,
                    outputs=[refundBox,botOperatorBox],
                    dataInputs=dataInputs,
                    fee=int(2e6),
                    sendChangeTo=Address.create(rewardAddress).getErgoAddress())

                return ('io.ergopad.vesting.refund_sold_out', unsignedTx)
            logging.info("Vest")
            roundInfo = self.roundInfo[proxyNFT]

            proxyBox = self.getProxyBox(proxyNFT)

            inputs = appKit.getBoxesById([proxyBox["boxId"],contributionBox["boxId"]])

            dataInputs = appKit.getBoxesById([self.oracle["boxId"]])
            proxyR4 = self.getRegister(proxyBox,"R4")

            tokenAmount = contributionBox["assets"][0]["amount"]

            remainingProxyTokens = {proxyBox["assets"][0]["tokenId"]: 1}
            if tokenAmount < proxyBox["assets"][1]["amount"]:
                remainingProxyTokens[proxyBox["assets"][1]["tokenId"]] = proxyBox["assets"][1]["amount"] - tokenAmount

            proxyVestingOutput = appKit.buildOutBox(
                value=proxyBox["value"],
                tokens=remainingProxyTokens,
                registers = [
                    proxyR4,
                    self.getRegister(proxyBox,"R5"),    #vestedTokenId
                    self.getRegister(proxyBox,"R6"),    #Seller address
                    self.getRegister(proxyBox,"R7")     #Whitelist tokenid
                ],
                contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(proxyBox["ergoTree"])))
            )

            vestingOutput = appKit.buildOutBox(
                value=int(1e6), 
                tokens={proxyBox["assets"][1]["tokenId"]: tokenAmount}, 
                registers=[       
                    ErgoAppKit.ergoValue([
                        proxyR4.getValue().apply(0), #Redeem period
                        proxyR4.getValue().apply(1),           #Number of periods
                        proxyR4.getValue().apply(2), #Start vesting april 1st
                        tokenAmount,         #Initial vesting amount
                        proxyR4.getValue().apply(6),
                        proxyR4.getValue().apply(7),
                        proxyR4.getValue().apply(8)
                    ], ErgoValueT.LongArray),            
                    #Vesting key
                    ErgoAppKit.ergoValue(proxyBox["boxId"], ErgoValueT.ByteArrayFromHex)                        
                ], 
                contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(vestingTree)))
            )

            userOutput = appKit.mintToken(
                value=int(5e5),
                tokenId=proxyBox["boxId"],
                tokenName=f"{roundInfo['name']} Vesting Key",
                tokenDesc=f'{{"Vesting Round": "{roundInfo["name"]}", "Vesting start": "{datetime.fromtimestamp(proxyR4.getValue().apply(2)/1000)}", "Periods": {proxyR4.getValue().apply(1)}, "Period length": "{timedelta(milliseconds=proxyR4.getValue().apply(0)).days} day(s)", "Total vested": {(tokenAmount*10**(-1*self.roundInfo[proxyBox["assets"][1]["tokenId"]]["decimals"]))} }}',
                mintAmount=1,
                decimals=0,
                contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(userTree)))
            )

            sellerOutput = appKit.buildOutBox(
                value=contributionBox["value"]-int(215e5),
                tokens=contributionTokens,
                registers=None,
                contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(self.getRegisterHex(proxyBox,"R6"))))
            )

            botOperatorOutput = appKit.buildOutBox(
                value=int(10e6),
                tokens=None,
                registers=None,
                contract=appKit.contractFromAddress(rewardAddress)
            )

            unsignedTx = appKit.buildUnsignedTransaction(
                inputs = inputs,
                dataInputs= dataInputs,
                outputs = [proxyVestingOutput,vestingOutput,userOutput,sellerOutput,botOperatorOutput],
                fee = int(10e6),
                sendChangeTo =appKit.contractFromAddress(rewardAddress).toAddress().getErgoAddress()
            )

            return ('io.ergopad.vesting.vest', unsignedTx)

        except Exception as e:
            logging.error(f'ERR:{e}')
            return ('error', None)

    def __str__(self):
        result = "Current Vesting state:\n"
        result += f"Proxy Boxes: {len(self._proxyBoxes)}\n"
        result += f"Contribution boxes: {len(self._contributionBoxes)}\n"
        result += f"Contribution boxes in mempool: {len(self.mempool.getUTXOsByTree(contributionTree))}\n"
        return result