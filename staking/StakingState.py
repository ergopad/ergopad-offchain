from cachetools import TTLCache

class StakingState:
    def __init__(self) -> None:
        self._stakeBoxes = {}
        self._mempool = TTLCache(maxsize=1000000,ttl=3600)
        self._spentMempoolBoxes = TTLCache(maxsize=1000000,ttl=3600)

    def addStakeBox(self, stakeBox) -> bool:
        mempool = "settlementHeight" not in stakeBox
        if mempool:
            self._mempool[stakeBox["additionalRegisters"]["R5"]] = stakeBox
            return True
        if stakeBox["additionalRegisters"]["R5"]["serializedValue"] in self._stakeBoxes:
            if stakeBox["settlementHeight"] <= self._stakeBoxes[stakeBox["additionalRegisters"]["R5"]["serializedValue"]]["settlementHeight"]:
                return False
        self._stakeBoxes[stakeBox["additionalRegisters"]["R5"]["serializedValue"]] = stakeBox
        return True

    def removeStakeBox(self, stakeBoxId):
        for stakeBox in self._stakeBoxes.keys():
            if self._stakeBoxes[stakeBox]["boxId"] == stakeBoxId:
                keyToRemove = stakeBox
        self._stakeBoxes[keyToRemove] = None
        self._mempool[keyToRemove] = None
        

    def spendBox(self,boxId):
        self._spentMempoolBoxes[boxId] = 1

    def __str__(self):
        result = "Current staking state:\n"
        result += f"Stake State: {self.stakeState['boxId']}\n"
        result += f"Emission: {self.emission['boxId']}\n"
        result += f"Stake Pool: {self.stakePool['boxId']}\n"
        result += f"Number of stake boxes: {len(self._stakeBoxes.keys())}\n"
        return result

    @property
    def stakeState(self):
        if self._mempool.get("stakeState") is not None:
            if self._spentMempoolBoxes.get(self._mempool["stakeState"]["boxId"]) is None:
                return self._mempool["stakeState"]
        return self._stakeState
    
    @stakeState.setter
    def stakeState(self, value):
        if "settlementHeight" in value:
            self._stakeState = value
        else:
            self._mempool["stakeState"] = value

    @property
    def emission(self):
        if self._mempool.get("emission") is not None:
            if self._spentMempoolBoxes.get(self._mempool["emission"]["boxId"]) is None:
                return self._mempool["emission"]
        return self._emission
    
    @emission.setter
    def emission(self, value):
        if "settlementHeight" in value:
            self._emission = value
        else:
            self._mempool["emission"] = value

    @property
    def stakePool(self):
        if self._mempool.get("stakePool") is not None:
            if self._spentMempoolBoxes.get(self._mempool["stakePool"]["boxId"]) is None:
                return self._mempool["stakePool"]
        return self._stakePool
    
    @stakePool.setter
    def stakePool(self, value):
        if "settlementHeight" in value:
            self._stakePool = value
        else:
            self._mempool["stakePool"] = value

    @property
    def cycle(self) -> int:
        return self._cycle

    @cycle.setter
    def cycle(self,value: int):
        self._cycle = value