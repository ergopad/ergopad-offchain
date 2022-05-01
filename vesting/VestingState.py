from datetime import datetime, timedelta
from time import time
from typing import List
import logging

import requests
from utils.Mempool import Mempool
from ergo_python_appkit.appkit import ErgoAppKit, ErgoValueT
from org.ergoplatform.appkit import Address, ErgoValue, OutBox
from special.collection import CollOverArray
from sigmastate.Values import ErgoTree

contributionTree = "1017040c0408040a040004000580dac40904000580dac40904000400040004020580ade20404000404058092f401040004000580bfd6060e20011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f0402040001009593b1a57300d802d601b2a5730100d602b2a5730200d196830301938cb2db6308b2a473030073040001e4c6a7060eed93c17201730593b1db630872017306ed93c17202730793b1db630872027308d809d601b2db6501fe730900d602db63087201d6038cb27202730a0001d604b2a5730b00d605ed93c17204730c93b1db63087204730dd606b2a5730e00d607ed93c17206730f93b1db630872067310d608b2a5731100d609eded93c1720899c1a7731293db63087208db6308a793c27208e4c6a7050e959372037313d19683040191e4c672010405e4c6a7040572057207720995937203e4c6a7060ed1968304018f8cb27202731400028cb2db6308a773150002720572077209d17316"
ergUsdOracleNFT = "011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f"
vestingTree = "1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206"
proxyTree = "10340400040004060402040004060408040204040400040004000e20011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f040404020e2003faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf040500050005c8010500050204000400040004020402040204020580897a0e205a50f4348840095fed4e84f94c6f1c0b540cc41d5c4dfc8b1f483e9c72315ecd010104000400040205000404040404060408040c040a040e040c041004020502040204000402040a04000400d805d601b2a5730000d602c27201d603c2a7d6049372027203d605e4c6a7060e957204d812d606b2db6501fe730100d607b2a5730200d608db63087207d609b2a5730300d60ab2db63087209730400d60b8c720a02d60ce4c6a70411d60d9d9c720bb2720c730500b2720c730600d60ec5a7d60fdb6308a7d610db63087201d611e4c6a7050ed612e4c6a7070ed613e4c672090411d614b27213730700d615b2a5730800d616b2db63087215730900d617b27208730a00d196830601938cb2db63087206730b0001730c929a9593b17208730dd801d618b27208730e0095938c721801730f8c721802731073119d9cc172077312e4c6720604059591720d7313720d73149683090193720ec5b2a473150093c1a7c17201937203720293b2720f731600b272107317009593b172107318938cb2720f73190002720b938cb27210731a0002998cb2720f731b0002720b93720ce4c672010411937211e4c67201050e937205e4c67201060e937212e4c67201070e96830d0193c17209731c93cbc27209731d938c720a017211731e93b27213731f00b2720c732000937214b2720c732100917214732293b27213732300b2720c73240093b27213732500720b93b27213732600b2720c73270093b27213732800b2720c73290093b27213732a00b2720c732b0093e4c67209050e720e9683030193c27215e4c6b2a4732c00050e938c721601720e938c721602732d9683030193c272077205938c7217017212938c721702720bd802d606b2a5732e00d607db6308a7d19683060193c27206720593c17206c1a793b2db63087206732f00b2720773300092db6903db6503feb2e4c6a70411733100720493b2db63087201733200b27207733300"

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
        contribBoxList.reverse()
        for box in contribBoxList:
            if not self.mempool.isSpent(box["boxId"]):
                return box

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

            refundThreshold = self.getRegister(contributionBox,"R4").getValue()
            userTree = self.getRegisterHex(contributionBox,"R5")
            proxyNFT = self.getRegisterHex(contributionBox,"R6")

            if int(self.oracle["additionalRegisters"]["R4"]["renderedValue"]) > refundThreshold:
                logging.info("refund oracle")
                inputs = appKit.getBoxesById([contributionBox["boxId"]])

                dataInputs = appKit.getBoxesById([self.oracle["boxId"]])

                refundBox = appKit.buildOutBox(
                    value = contributionBox["value"]-int(7e6),
                    tokens={
                        contributionBox["assets"][0]["tokenId"]: contributionBox["assets"][0]["amount"]
                    },
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

            if self.getProxyBox(proxyNFT)["assets"][1]["amount"] < contributionBox["assets"][0]["amount"]:
                logging.info("refund sold out")
                inputs = appKit.getBoxesById([contributionBox["boxId"]])

                dataInputs = appKit.getBoxesById([self.getProxyBox(proxyNFT)["boxId"]])

                refundBox = appKit.buildOutBox(
                    value = contributionBox["value"]-int(7e6),
                    tokens={
                        contributionBox["assets"][0]["tokenId"]: contributionBox["assets"][0]["amount"]
                    },
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
                value=int(1e6),
                tokenId=proxyBox["boxId"],
                tokenName=f"{roundInfo['name']} Vesting Key",
                tokenDesc=f'{{"Vesting Round": "{roundInfo["name"]}", "Vesting start": "{datetime.fromtimestamp(proxyR4.getValue().apply(2)/1000)}", "Periods": {proxyR4.getValue().apply(1)}, "Period length": "{timedelta(milliseconds=proxyR4.getValue().apply(0)).days} day(s)", "Total vested": {(tokenAmount*10**(-1*self.roundInfo[proxyBox["assets"][1]["tokenId"]]["decimals"]))} }}',
                mintAmount=1,
                decimals=0,
                contract=appKit.contractFromTree(appKit.treeFromBytes(bytes.fromhex(userTree)))
            )

            sellerOutput = appKit.buildOutBox(
                value=contributionBox["value"]-int(22e6),
                tokens={contributionBox["assets"][0]["tokenId"]: tokenAmount},
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

    def __str__(self):
        result = "Current Vesting state:\n"
        result += f"Proxy Boxes: {len(self._proxyBoxes)}\n"
        result += f"Contribution boxes: {len(self._contributionBoxes)}\n"
        result += f"Contribution boxes in mempool: {len(self.mempool.getUTXOsByTree(contributionTree))}\n"
        return result