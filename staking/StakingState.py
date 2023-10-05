from time import time
from typing import List
import logging
import os
from utils.Mempool import Mempool
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValueT
from org.ergoplatform.appkit import Address, ErgoValue, OutBox

stakeStateNFT = os.getenv("STAKE_STATE_NFT")
stakePoolNFT = os.getenv("STAKE_POOL_NFT")
emissionNFT = os.getenv("EMISSION_NFT")
stakeTokenID =  os.getenv("STAKE_TOKEN_ID")
stakedTokenID = os.getenv("STAKED_TOKEN_ID")
incentiveAddress = os.getenv("INCENTIVE_ADDRESS")
incentiveTree = os.getenv("INCENTIVE_TREE")

class StakingState:
    def __init__(self) -> None:
        self._stakeBoxes = {}
        self._incentiveBoxes = {}
        self.mempool: Mempool = Mempool()

    def getR4(self,box):
        hexVal = ""
        if "serializedValue" in box["additionalRegisters"]["R4"]:
            hexVal = box["additionalRegisters"]["R4"]["serializedValue"]
        else:
            hexVal = box["additionalRegisters"]["R4"]
        return ErgoValue.fromHex(hexVal).getValue()
    
    def nextCycleTime(self):
        r4 = self.getR4(self.stakeState)
        return r4.apply(3)+r4.apply(4)

    def addStakeBox(self, stakeBox) -> bool:
        mempool = "settlementHeight" not in stakeBox and "inclusionHeight" not in stakeBox
        if not mempool:
            r5 = stakeBox["additionalRegisters"]["R5"]["serializedValue"][4:] if "serializedValue" in stakeBox["additionalRegisters"]["R5"] else stakeBox["additionalRegisters"]["R5"][4:]
            if r5 in self._stakeBoxes:
                if stakeBox["settlementHeight"] <= self._stakeBoxes[r5].get("settlementHeight", self._stakeBoxes[r5]["inclusionHeight"]):
                    return False
            self._stakeBoxes[r5] = stakeBox
            return True
        else:
            False

    def removeStakeBox(self, stakeBoxId):
        keyToRemove = None
        for stakeBox in self._stakeBoxes.keys():
            if self._stakeBoxes[stakeBox]["boxId"] == stakeBoxId:
                keyToRemove = stakeBox
        if keyToRemove is not None:
            self._stakeBoxes.pop(keyToRemove,None)

    def addIncentiveBox(self, incentiveBox) -> bool:
        mempool = "settlementHeight" not in incentiveBox
        if not mempool:
            self._incentiveBoxes[incentiveBox["boxId"]] = incentiveBox
            return True
        return False

    def removeIncentiveBox(self, incentiveBoxId):
        self._incentiveBoxes.pop(incentiveBoxId,None)

    def getIncentiveBox(self, value: int):
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(incentiveTree):
            if not self.mempool.isSpent(box["boxId"]) and box["value"] > value + 100000:
                return box

    def incentiveTotal(self):
        total = 0
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(incentiveTree):
            if not self.mempool.isSpent(box["boxId"]):    
                total += box["value"]
        return total

    def newTx(self, tx):
        isMempool = "globalIndex" not in tx
        if isMempool:
            self.mempool.addTx(tx)
        else:
            self.mempool.removeTx(tx["id"])
            for box in tx["inputs"]:
                self.removeIncentiveBox(box["boxId"])
                self.removeStakeBox(box["boxId"])


    def __str__(self):
        result = "Current staking state:\n"
        result += f"Stake State: {self.stakeState['boxId']}\n"
        result += f"Emission: {self.emission['boxId']}\n"
        result += f"Stake Pool: {self.stakePool['boxId']}\n"
        result += f"Number of stake boxes: {len(self._stakeBoxes.keys())}\n"
        result += f"Incentive total: {self.incentiveTotal()}\n"
        return result

    def compoundTX(self, appKit: ErgoAppKit, rewardAddress: str):
        try:
            stakeBoxes = []
            stakeBoxesOutput = []
            totalReward = 0
            # emmission box contains current staking info        
            emissionR4 = self.getR4(self.emission)
            # emission box R4[2] contains current remaining stakers
            if emissionR4.apply(2) <= 0: 
                #logging.info("Remaining stakers: 0")
                return

            for box in self._stakeBoxes.values():
                boxR4 = self.getR4(box)
                if boxR4.apply(0) == emissionR4.apply(1) and not self.mempool.isSpent(box["boxId"]) and box["boxId"]!="3230c19953f9a4f1b887f45598ae691c16e5aceda8e319c8d5a80c5d3a5060b3" and box["boxId"]!="86a9b423bfe23b5359466a2b5aa67a9c8e6b1c0f2d9e1b3e4a8d4234f533ed98" and box["boxId"]!="3caba90e59c5d78ecb99ca472cd74f5620c3197c7cf4b3b47ded6d31d93608c7":
                    # calc rewards and build tx
                    stakeBoxes.append(box["boxId"])
                    stakeReward = int(box["assets"][1]["amount"] * emissionR4.apply(3) / emissionR4.apply(0))
                    totalReward += stakeReward
                    stakeBoxesOutput.append(appKit.buildOutBox(
                        value=box["value"],
                        tokens={
                            box["assets"][0]["tokenId"]: 1,
                            box["assets"][1]["tokenId"]: box["assets"][1]["amount"] + stakeReward
                        },
                        registers=[
                            ErgoAppKit.ergoValue([boxR4.apply(0)+1,boxR4.apply(1)],ErgoValueT.LongArray),
                            ErgoAppKit.ergoValue(box["additionalRegisters"]["R5"]["renderedValue"] if "renderedValue" in box["additionalRegisters"]["R5"] else box["additionalRegisters"]["R5"][4:],ErgoValueT.ByteArrayFromHex)
                        ],
                        contract=appKit.contractFromAddress(box["address"])
                    ))

                # every <numBoxes>, go ahead and submit tx
                if len(stakeBoxes)>=30:
                    logging.info("found 30")
                    break
            
            if len(stakeBoxes) == 0:
                return
            emissionAssets = {
                            self.emission["assets"][0]["tokenId"]: 1
                        }
            if totalReward < self.emission["assets"][1]["amount"]:
                emissionAssets[self.emission["assets"][1]["tokenId"]] = self.emission["assets"][1]["amount"]-totalReward

            txValue = int(900000 + (750000 * len(stakeBoxes)))
            incentiveBox = self.getIncentiveBox(txValue)
            txFee = int(max(int(1e6),(int(1e6)+int(5e5)*len(stakeBoxesOutput))))
            logging.info(self.emission["boxId"])
            logging.info(incentiveBox["boxId"])
            logging.info(stakeBoxes)
            inputs = appKit.getBoxesById([self.emission["boxId"]]+stakeBoxes+[incentiveBox["boxId"]])
            logging.info("check 2")
            emissionOutput = appKit.buildOutBox(
                value=self.emission["value"],
                tokens=emissionAssets,
                registers=[ErgoAppKit.ergoValue([
                    emissionR4.apply(0),
                    emissionR4.apply(1),
                    emissionR4.apply(2)-len(stakeBoxes),
                    emissionR4.apply(3)],ErgoValueT.LongArray)
                ],
                contract=appKit.contractFromTree(inputs[0].getErgoTree()))

            incentiveOutput = appKit.buildOutBox(
                value=incentiveBox["value"]-txValue,
                tokens=None,
                registers=None,
                contract=appKit.contractFromTree(inputs[-1].getErgoTree())
            )

            rewardOutput = appKit.buildOutBox(
                value=txValue-txFee,
                tokens=None,
                registers=None,
                contract=appKit.contractFromAddress(rewardAddress)
            )

            unsignedTx = appKit.buildUnsignedTransaction(
                inputs=inputs,
                outputs=[emissionOutput]+stakeBoxesOutput + [incentiveOutput,rewardOutput],
                fee=txFee,
                sendChangeTo=Address.create(rewardAddress).getErgoAddress()
            )

            return unsignedTx

        except Exception as e:
            logging.error(f'ERR:{e}')

    def emitTransaction(self, appKit: ErgoAppKit, rewardAddress: str):
        if self.nextCycleTime() < time()*1000:
            stakeStateInput = appKit.getBoxesById([self.stakeState["boxId"]])[0]
            stakePoolInput = appKit.getBoxesById([self.stakePool["boxId"]])[0]
            emissionInput = appKit.getBoxesById([self.emission["boxId"]])[0]
            incentiveInput = appKit.getBoxesById([self.getIncentiveBox(int(5e6))["boxId"]])[0]

            stakeStateR4 = self.getR4(self.stakeState)
            stakePoolR4 = self.getR4(self.stakePool)
            emissionR4 = self.getR4(self.emission)
            if emissionR4.apply(2) > 0:
                raise Exception("Previous emit not finished yet")

            newStakePoolAmount = self.stakePool["assets"][1]["amount"] - stakePoolR4.apply(0)
            dust = 0
            if len(self.emission["assets"]) > 1:
                dust = self.emission["assets"][1]["amount"]
                newStakePoolAmount += dust
            stakeStateOutput = appKit.buildOutBox(
                value=self.stakeState["value"],
                tokens={
                    self.stakeState['assets'][0]['tokenId']: self.stakeState['assets'][0]['amount'],
                    self.stakeState['assets'][1]['tokenId']: self.stakeState['assets'][1]['amount']
                },
                registers=[
                    ErgoAppKit.ergoValue([
                        int(stakeStateR4.apply(0)+stakePoolR4.apply(0)-dust),
                        int(stakeStateR4.apply(1)+1),
                        int(stakeStateR4.apply(2)),
                        int(stakeStateR4.apply(3)+stakeStateR4.apply(4)),
                        int(stakeStateR4.apply(4))
                    ],ErgoValueT.LongArray)
                ],
                contract=appKit.contractFromTree(stakeStateInput.getErgoTree())
                )

            stakePoolOutput = appKit.buildOutBox(
                value=self.stakePool["value"],
                tokens={
                    self.stakePool['assets'][0]['tokenId']: self.stakePool['assets'][0]['amount'],
                    self.stakePool['assets'][1]['tokenId']: newStakePoolAmount
                },
                registers=[
                    stakePoolInput.getRegisters()[0]
                ],
                contract=appKit.contractFromTree(stakePoolInput.getErgoTree())
                )

            emissionOutput = appKit.buildOutBox(
                value=self.emission["value"],
                tokens={
                    self.emission['assets'][0]['tokenId']: self.emission['assets'][0]['amount'],
                    self.stakePool['assets'][1]['tokenId']: stakePoolR4.apply(0)
                },
                registers=[
                    ErgoAppKit.ergoValue([
                        int(stakeStateR4.apply(0)),
                        int(stakeStateR4.apply(1)),
                        int(stakeStateR4.apply(2)),
                        int(stakePoolR4.apply(0))
                    ],ErgoValueT.LongArray)
                ],
                contract=appKit.contractFromTree(emissionInput.getErgoTree())
                )

            incentiveOutput = appKit.buildOutBox(
                value=incentiveInput.getValue()-int(4e6),
                tokens=None,
                registers=None,
                contract=appKit.contractFromTree(incentiveInput.getErgoTree())
            )

            rewardOutput = appKit.buildOutBox(
                value=int(3e6),
                tokens=None,
                registers=None,
                contract=appKit.contractFromAddress(rewardAddress)
            )
            logging.info("hmmm")
            unsignedTx = appKit.buildUnsignedTransaction(
                inputs=[stakeStateInput,stakePoolInput,emissionInput,incentiveInput],
                outputs=[stakeStateOutput,stakePoolOutput,emissionOutput,incentiveOutput,rewardOutput],
                fee=int(1e6),
                sendChangeTo=Address.create(rewardAddress).getErgoAddress(),
                preHeader=appKit.preHeader()
            )

            return unsignedTx

    def consolidateTransaction(self, appKit: ErgoAppKit, rewardAddres: str):
        dustBoxes = []
        dustTotal = 0
        for box in list(self._incentiveBoxes.values()) + self.mempool.getUTXOsByTree(incentiveTree):
            if box["boxId"] not in dustBoxes and box["value"] < 10000000 and not self.mempool.isSpent(box["boxId"]):
                dustBoxes.append(box["boxId"])
                dustTotal += box["value"]

        if len(dustBoxes) >= 2:
            logging.info(len(dustBoxes))
            inputs = appKit.getBoxesById(dustBoxes)

            incentiveOutput = appKit.buildOutBox(
                value=dustTotal-int(1e6)-int(5e5*len(dustBoxes)),
                tokens=None,
                registers=None,
                contract=appKit.contractFromAddress(incentiveAddress)
            )         

            rewardOutput = appKit.buildOutBox(
                value=int(5e5*len(dustBoxes)),
                tokens=None,
                registers=None,
                contract=appKit.contractFromAddress(rewardAddres)
            )

            unsignedTx = appKit.buildUnsignedTransaction(
                inputs=inputs,
                outputs=[incentiveOutput,rewardOutput],
                fee=int(1e6),
                sendChangeTo=Address.create(rewardAddres).getErgoAddress(),
                preHeader=appKit.preHeader()
            )

            return unsignedTx

    @property
    def stakeState(self):
        if self.mempool.getUTXOByTokenId(stakeStateNFT) is not None:
            return self.mempool.getUTXOByTokenId(stakeStateNFT)
        return self._stakeState
    
    @stakeState.setter
    def stakeState(self, value):
        if "settlementHeight" in value:
            self._stakeState = value

    @property
    def emission(self):
        if self.mempool.getUTXOByTokenId(emissionNFT) is not None:
            return self.mempool.getUTXOByTokenId(emissionNFT)
        return self._emission
    
    @emission.setter
    def emission(self, value):
        if "settlementHeight" in value:
            self._emission = value

    @property
    def stakePool(self):
        if self.mempool.getUTXOByTokenId(stakePoolNFT) is not None:
            return self.mempool.getUTXOByTokenId(stakePoolNFT)
        return self._stakePool
    
    @stakePool.setter
    def stakePool(self, value):
        if "settlementHeight" in value:
            self._stakePool = value

    @property
    def cycle(self) -> int:
        return self._cycle

    @cycle.setter
    def cycle(self,value: int):
        self._cycle = value