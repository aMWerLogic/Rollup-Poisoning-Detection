#IT IS USED TO EXTRACT NAME SYMBOL AND DECIMALS OF A SMART CONTRACT BY ADDRESS
from web3 import Web3
from requests.exceptions import HTTPError
import time

class GetTokens:
    def __init__(self, RPC= None):
        self.RPC = RPC
        self.w3 = Web3(Web3.HTTPProvider(RPC))
        self.ERC20_ABI = [
            {"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
            {"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
            {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"type":"function"},
        ]

    def get_token_name_symbol(self,address):
        try:
            contract = self.w3.eth.contract(address=address, abi=self.ERC20_ABI)
            name = contract.functions.name().call()
            symbol = contract.functions.symbol().call()
            decimals = contract.functions.decimals().call()
            return name, symbol, decimals
        except Exception as e:
            return None, None, None

    def get_token_info(self, address):
        contract = self.w3.eth.contract(address=Web3.to_checksum_address(address), abi=self.ERC20_ABI)
        try:
            name = contract.functions.name().call()
        except HTTPError as e:
            raise e
        except Exception as e:
            #print(f"contract: {contract}, get_token_info error: {e}")
            name = None
        try:
            symbol = contract.functions.symbol().call()
        except HTTPError as e:
            raise e
        except Exception as e:
            #print(f"contract: {contract}, get_token_info error: {e}")
            symbol = None
        try:
            decimals = contract.functions.decimals().call()
        except HTTPError as e:
            raise e
        except Exception as e:
            #print(f"contract: {contract}, get_token_info error: {e}")
            #Try with uint256 ABI if uint8 fails
            try:
                DECIMALS_ABI_256 = [{"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"type":"function"}]
                contract256 = self.w3.eth.contract(address=Web3.to_checksum_address(address), abi=DECIMALS_ABI_256)
                decimals = contract256.functions.decimals().call()
            except HTTPError as e:
                raise e
            except Exception:
                print(f"contract: {address}, get_token_info error: {e}")
                decimals = None
        return name, symbol, decimals
    

    def safe_get_token_info(self, addr, wait_time=3, info=0):
        i=1
        while True:
            try:
                return self.get_token_info(addr)
            except HTTPError as e:
                if info==1:
                    print(f"Network error {e}, retrying in {wait_time*i} seconds...")
                time.sleep(wait_time*i)
                i+=1
                continue
    
