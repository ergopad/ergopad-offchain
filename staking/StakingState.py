from time import time
from typing import List
import logging
from utils.Mempool import Mempool
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValueT
from org.ergoplatform.appkit import Address, ErgoValue, OutBox

stakeStateNFT = "05cde13424a7972fbcd0b43fccbb5e501b1f75302175178fc86d8f243f3f3125"
stakePoolNFT = "0d01f2f0b3254b4a1731e1b61ad77641fe54de2bd68d1c6fc1ae4e7e9ddfb212"
emissionNFT = "0549ea3374a36b7a22a803766af732e61798463c3332c5f6d86c8ab9195eed59"
stakeTokenID =  "1028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee"
stakedTokenID = "d71693c49a84fbbecd4908c94813b46514b18b67a99952dc1e6e4791556de413"
incentiveAddress = "ciYus5z3iMQUYtVHoGEN4rxgv26ddxnamW8uyLV4uJdsis4NRRJ2Jy58ja5dP8wr88Qa7q3rMZVQsnX1sru2R54bwqL3xGv52v2EezZzMLX1XDTqKdQsPEGFD3G7UptKSm7T19oqe4W56pabRFmDmpKb7gjgG6QVeVAVR6cEEPtA7TaHPbGM5Hih2kxJLAmtofFiXcCQXwWgFjkDdRK9AcsARTa2wtBF9cLoQepfgAMuuXa5Be1EfsKYUfFuFviB8tWqosLw9DJSZWM3n6bLyBKysMtngXPaSDxQn7gGCbjHeVmoDHiiqifFgNFPRUrvdA62WgbGS2xJrWAR7UFMgGc4QvfKuT1j28BsVBetttfWck1oBYuq6tCtyrxvaFWz5V7g8z6A6KX2SQMcPhvKLx9om8F6oo2r4ZegWFVSUeXsmUCt3zE5RZbDoCowtLn2SQMf9Wxw8inPJqzesGHLwJY9vchXnUdYmaJSUCuH8w9pd8n29qRSR28WCgW6Hg8Vfcf5x4ybNuowjca98zfYpLJPAfQUmEbWMQ4YAPbKQCYg6zZ8QVZduWuVVMuHVEH1Qf52yfKrqqrw7mgTNvBoxUEFyQgHXqiQpVAxppfvhJgzTA3H9Dxzz8CBqVFzCmnnXAMfafcUgVabRmvV4PjCUWM4sW19cHbTrtVJ9iYarPNwjR7C1Yd18CqpsGnvjcyo87dDBvxGxSt9"
incentiveTree = "101e04000e20c66a7fdc803a68ca52666aa40b056949dec6242deb6dd7855733b6648d7a049c0e203fb016f4eb36ff7b2226fc5ee456b5d54d05e22849ff53599215aefc87a68b350404040604020580a4e803040805809bee020402040004000e201028de73d018f0c9a374b71555c5b8f1390994f2f41633e7b9d68f77735782ee04000402010004c0ee6d04e0c65b0480897a04c0843d0402040404060400058084af5f040204c0843d04040580897a0100d803d601b2a4730000d602cbc27201d603730195ed937202730293cbc2b2a47303007203d801d604b2a5730400d19683040193c5b2a499b1a4730500c5a793c27204c2a792c1720499c1a7730693c1b2a57307007308959372027203d806d604b1a4d6059972047309d606b2a5720500d607b1b5a4d9010763d801d609db630872079591b17209730aed938cb27209730b0001730c93b2e4c672070411730d00b2e4c672010411730e00730fd6089a73109c73117207d6099a73129c73137207d19683060193c5b2a4720500c5a793c27206c2a792c1720699c1a77e72080593c1b2a57204007e99720872090593c1b2a59a72047314007e7209059372049a720773159593b1a57316d803d604b2a5731700d605c2a7d606c1a7d19683050193c27204720591c172047206907206731893c1b2a57319007e9c731ab1b5a4d901076393c2720772050593c1b2a5731b00731cd1731d"

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
        mempool = "settlementHeight" not in stakeBox
        if not mempool:
            if stakeBox["additionalRegisters"]["R5"]["serializedValue"] in self._stakeBoxes:
                if stakeBox["settlementHeight"] <= self._stakeBoxes[stakeBox["additionalRegisters"]["R5"]["serializedValue"]]["settlementHeight"]:
                    return False
            self._stakeBoxes[stakeBox["additionalRegisters"]["R5"]["serializedValue"]] = stakeBox
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
                if boxR4.apply(0) == emissionR4.apply(1) and not self.mempool.isSpent(box["boxId"]):
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
                            ErgoAppKit.ergoValue(box["additionalRegisters"]["R5"]["renderedValue"],ErgoValueT.ByteArrayFromHex)
                        ],
                        contract=appKit.contractFromAddress(box["address"])
                    ))

                # every <numBoxes>, go ahead and submit tx
                if len(stakeBoxes)>=50:
                    logging.info("found 50")
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
            logging.info("check 1")
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
            if box["boxId"] not in dustBoxes and box["value"] < 100000000 and not self.mempool.isSpent(box["boxId"]):
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