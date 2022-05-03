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

contributionTree = "1019040c0408040a040004000580dac40904000580dac40904000400040004020580ade20404000404058092f401040004000580bfd6060e20011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f040201010402040001009593b1a57300d802d601b2a5730100d602b2a5730200d196830301938cb2db6308b2a473030073040001e4c6a7060eed93c17201730593b1db630872017306ed93c17202730793b1db630872027308d809d601b2db6501fe730900d602db63087201d6038cb27202730a0001d604b2a5730b00d605ed93c17204730c93b1db63087204730dd606b2a5730e00d607ed93c17206730f93b1db630872067310d608b2a5731100d609eded93c1720899c1a7731293db63087208db6308a793c27208e4c6a7050e959372037313d19683040191e4c672010405e4c6a7040572057207720995937203e4c6a7060ed1968304019593b17202731473158f8cb27202731600028cb2db6308a773170002720572077209d17318"
ergUsdOracleNFT = "011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f"
vestingTree = "1012040204000404040004020406040c0408040a050004000402040204000400040404000400d812d601b2a4730000d602e4c6a7050ed603b2db6308a7730100d6048c720302d605db6903db6503fed606e4c6a70411d6079d997205b27206730200b27206730300d608b27206730400d609b27206730500d60a9972097204d60b95917205b272067306009d9c7209b27206730700b272067308007309d60c959272077208997209720a999a9d9c7207997209720b7208720b720ad60d937204720cd60e95720db2a5730a00b2a5730b00d60fdb6308720ed610b2720f730c00d6118c720301d612b2a5730d00d1eded96830201aedb63087201d901134d0e938c721301720293c5b2a4730e00c5a79683050193c2720ec2720193b1720f730f938cb2720f731000017202938c7210017211938c721002720cec720dd801d613b2db630872127311009683060193c17212c1a793c27212c2a7938c7213017211938c721302997204720c93e4c67212050e720293e4c6721204117206"
proxyTree = "103404020e205a50f4348840095fed4e84f94c6f1c0b540cc41d5c4dfc8b1f483e9c72315ecd040004060400040604080400040204040400040004000e20011d3364de07e5a26f0c4eef0852cddb387039a921b7154ef3cab22c6eda887f040404020e2003faf2cb329f2e90d6d23b58d91bbb6c046aa143261cc21f52fbe2824bfcbf040500050005c8010500050204000400040004020402040204020580897a010104000400040205000404040404060408040c040a040e040c041004020502040004000402040a04000400d804d601b2a5730000d602c27201d60393cb72027301d604e4c6a7060e957203d812d605b2db6501fe730200d606b2a5730300d607db63087206d608b2db63087201730400d6098c720802d60ae4c6a70411d60b9d9c7209b2720a730500b2720a730600d60cc5a7d60db2a5730700d60edb6308a7d60fdb6308720dd610e4c6a7050ed611e4c6a7070ed612e4c672010411d613b27212730800d614b2a5730900d615b2db63087214730a00d616b27207730b00d196830601938cb2db63087205730c0001730d929a9593b17207730ed801d617b27207730f0095938c72170173108c721702731173129d9cc172067313e4c6720504059591720b7314720b73159683090193720cc5b2a473160093c1a7c1720d93c2a7c2720d93b2720e731700b2720f7318009593b1720f7319938cb2720e731a00027209938cb2720f731b0002998cb2720e731c0002720993720ae4c6720d0411937210e4c6720d050e937204e4c6720d060e937211e4c6720d070e96830d0193c17201731d7203938c7208017210731e93b27212731f00b2720a732000937213b2720a732100917213732293b27212732300b2720a73240093b27212732500720993b27212732600b2720a73270093b27212732800b2720a73290093b27212732a00b2720a732b0093e4c67201050e720c9683030193c27214e4c6b2a4732c00050e938c721501720c938c721502732d9683030193c272067204938c7216017211938c7216027209d802d605db6308a7d606b2a5732e00d196830601937202720493c17201c1a793b2db63087201732f00b2720573300092db6903db6503feb2e4c6a7041173310093c27206c2a793b2db63087206733200b27205733300"

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
                value=contributionBox["value"]-int(185e5),
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