def to_ethereum_address(hex_string):
    return "0x" + hex_string[-40:]

def convert_to_fixed_size(address):
    address = address[2:]
    if len(address) != 40:
        raise ValueError("Address should be 40 characters long without the '0x' prefix.")
    fixed_size_address = '0x' + address.zfill(64)
    return fixed_size_address.lower()