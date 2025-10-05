import requests
import requests
import csv
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")

API_KEY = os.getenv("etherscanToken")
contracts = [
    "0xe362117f6011418faeb6b53e2d5a5049af579ea9",
    "0x469f4d540c01283dca9a10b2a8b6104aa43540cc",
    "0xa52cd97c022e5373ee305010ff2263d29bb87a70",
    "0x2ce950fbe1777bf25b30edbaeb15aabd552eef1b",
    "0x7b2d44c3aaf4c8c688f200d2ede6e1f01c3919dc",
    "0x174217619ef8fe9a0a77e18599f2e0a8b46c6080",
    "0xba16f78696b0a0acfa68e4427a4f3ea176aa626f",
    "0x633edc9b25f2d908644146b64bb96d069ae26f0a",
    "0xc2daaf2df7e39f697780b8098210ba90880c084a",
    "0x2709f1a7666df3e2c76895fab93c61e80a564a82",
    "0xe82d16b884bfdacf2a1a0171d6b7d0a59322ddc9",
    "0x686fc609cdfb11be74baa3d6b410b8e30c8f1a03",
    "0x1282bd05c6cb7c25f59b0f6b448735038b3c1639",
    "0x32e079db818bce199338dcdb6829a039a22d1698",
    "0xae321792046a4606ab5965793a61c0a7a703ed7a",
    "0xd0f3a5d405537f13c4b1070fb5ed13fb820fbbb2",
    "0xcdb94376e0330b13f5becaece169602cbb14399c",
    "0x708b4e21b0ffd30f73704cb7fc358290e8df4c2a",
    "0xd3823bee2354fa8a5d8133c001947d688b896e0b",
    "0x23421a01f7db8a87c3b03df0dade06f0c2203ac1",
    "0x48415ef4091627f07be038c2efee76752c07e1b3",
    "0x5c61dc9507422ed7dcd266806c59e8eb9b63b0ca",
    "0x2079287b6756d1bbaee677f8e84c9aceb4639eb7",
    "0x340a523dcd4a636730cd27f58cf25e14a9497083",
    "0x95e9205d8de2a300efc75ca29af0be5429a0a8e5",
    "0x1108f83b78548a528fa4d5baf994f06927a17f05",
    "0x08fa12051c513a859cd2f20513a169c4a62082f9",
    "0x3998d5afff01d827e0d5a56ba71c509a457e4a5b",
    "0x1af8fd0ddbfe734422c3b65be5c0b0a92f493d46",
    "0x583700c93d12637eaef7b241886123ae1d39e7d2",
    "0xffccf8c057a6f213b149a61ff91eec77ec56946e",
    "0x60e64eff277a5b82a82e8b7bf34897e6ed0f9829",
    "0xcb3e05c74f3c120e86122a3236b4035185f2f633",
    "0x082ea095ccf5cc3c536c667287f695c483113a21",
    "0xd01fb767f70c5aa387c4b53e52eddad6502021cf",
    "0x5060540b6ea17b0250acb5d74e57881e05b7cefa",
    "0x0818736a5b40b9a2e86fccec2f5f2e69784b5b80",
    "0xace9373de31110e7bca81dffd91fa9e883e2bfef",
    "0x5de6ea97cb1094cf6c08f44bb40dc3a6814b721f",
    "0xb005f342246f8a4599d43f3d359d8e46fedb813d",
    "0xb9820b49ebf3fbfe46f1624ccaab218c623fce0a",
    "0x0b36bdd094eab2c8f52ddb8371728b6d3f99e27b",
    "0x0f78bb4668d6375577c788bbc903a38b789a198c",
    "0xb85c6fbf780eb913dc34aa0b6f74d1e6a15c356f",
    "0xdee46be9d0b207e5d88d2efd84a045e725a242f7",
    "0x276d286f2d820b9ed8e054919e0b4a289a72d240",
    "0xed28323e664c9a8e9b17bfeb423b9a8fcfc4daae",
    "0xf552a560871d617dcd3f7f5009e2e9e0cb470ca2",
    "0xabab6725ca886ef37d377803fa4a6d9367ed0000",
    "0x6982bbaa3fcf743d6615585ba9bec3c3ee5d1933",
    "0x20adf5e414ecb7b561b0c2a1e6fe27d5d0b36134",
    "0x0824b240c36ed6bb452646d2a6316ac234c4ffd8",
    "0x83c62e6085abc3a8f8e1d86ca67ee88c99f53645",
    "0x11e776e7034cf2ffb21f96a0164cdae82afa670a",
    "0x2dfc6c3aae5d8bd9e61a886df055f55083448a6e",
    "0xdd77d38de83ceea042aef3015c72825efa40f8c5",
    "0x1010e1da0d3184c852c0a18705723a63923b0101",
    "0xa54b8e178a49f8e5405a4d44bb31f496e5564a05",
    "0x2054c9168e37b5e0e65820a1718717073852bb02",
    "0xfc44abe4f62122d31e3ff317d60f7bbce7e7b7db",
    "0xee43369197f78cfdf0d8fc48d296964c50ac7b57",
    "0xd69d0877e0075e80e1e26635bc4d9452939b8399",
    "0x1e25ad7672b6919601daca207c279d6c290c2045",
    "0x039207c3c09aa6baa86e613ef7b485178e171561",
    "0xf0cb2dc0db5e6c66b9a70ac27b06b878da017028",
    "0x35e6a59f786d9266c7961ea28c7b768b33959cbb",
    "0x6685a287dd2502b043c0894e6834837cc0745860",
    "0x3082cc23568ea640225c2467653db90e9250aaa0",
    "0x2d9c0e1b4f97efc6aa8984ddc632cfc50f8b212d",
    "0x83c060ffdb325e8ddb546737283e351d08271b91",
    "0x74885b4d524d497261259b38900f54e6dbad2210",
    "0x1263fea931b86f3e8ce8afbf29f66631b7be9347",
    "0x68890bd34b01b240c6ce9f0a5829929f89c5e4d1",
    "0x2bc8e60b83c7bb078df2a6bd9f0ab38f4e3d6351",
    "0xbafbcb010d920e0dab9dfdcf634de1b777028a85",
    "0xb3c624163bd975f772d3c4bce8f1611402bfd1b0"
]

results = []

for c in contracts:
    url = f"https://api.etherscan.io/v2/api?chainid=42161&module=contract&action=getsourcecode&address={c}&apikey={API_KEY}"
    response = requests.get(url)
    
    try:
        r = response.json()
    except ValueError:
        print(f"{c}: Failed to parse JSON response")
        results.append((c, False))
        continue
    
    if 'result' in r and len(r['result']) > 0:
        result_entry = r['result'][0]
        if isinstance(result_entry, dict):
            source_code = result_entry.get('SourceCode', '')
        else:
            source_code = result_entry

        verified = bool(source_code)
        print(f"{c}: {'Verified' if verified else 'Not Verified'}")
        results.append((c, verified))
    else:
        print(f"{c}: No result found")
        results.append((c, False))
with open('contracts_verification.csv', 'w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(['address', 'verified'])
    writer.writerows(results)

print("Results saved to contracts_verification.csv")