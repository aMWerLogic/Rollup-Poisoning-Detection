#IT IS USED TO EXTRACT NAME SYMBOL AND DECIMALS OF A SMART CONTRACT BY ADDRESS
from web3 import Web3

ZKSYNC_RPC = "https://mainnet.era.zksync.io"
w3 = Web3(Web3.HTTPProvider(ZKSYNC_RPC))

ERC20_ABI = [
    {"constant":True,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
    {"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
    {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"type":"function"},
]

def get_token_name_symbol(address):
    try:
        contract = w3.eth.contract(address=address, abi=ERC20_ABI)
        name = contract.functions.name().call()
        symbol = contract.functions.symbol().call()
        decimals = contract.functions.decimals().call()
        return name, symbol, decimals
    except Exception as e:
        return None, None, None
    
def get_token_info(address):
    contract = w3.eth.contract(address=Web3.to_checksum_address(address), abi=ERC20_ABI)
    try:
        name = contract.functions.name().call()
    except Exception:
        name = None
    try:
        symbol = contract.functions.symbol().call()
    except Exception:
        symbol = None
    try:
        decimals = contract.functions.decimals().call()
    except Exception:
        #Try with uint256 ABI if uint8 fails
        try:
            DECIMALS_ABI_256 = [{"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint256"}],"type":"function"}]
            contract256 = w3.eth.contract(address=Web3.to_checksum_address(address), abi=DECIMALS_ABI_256)
            decimals = contract256.functions.decimals().call()
        except Exception:
            decimals = None
    return name, symbol, decimals