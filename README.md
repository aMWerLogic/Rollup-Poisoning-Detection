Steps to perform detection:

1. Run getData/get_rollup_data.py to download ERC-20 data from 3xpl (with token available for academic purposes), then extract tsv files with getData/extract.py and then apply getData/to_parquet.py to convert files into parquets.
2. Use scrap_arb_tokens.py to obtain legitimate token list for a specified rollup (use proper etherscan endpoints and find appropriate date for web.archive.org). 
3. Use extractPrices.py to obtain legitimate token prices for a specified date.
4. Run main.py [rollup] to perform steps0 -> 5 of detection system.
5. In results dir run analize_results.py for further analisys.
6. Use retrive_contract.py to obtain contract addresses for fake transfers.
7. Having those suspected fake addresses, run verify_contracts.py to obtain information if a given contract is verified on etherscan; if it is then manually check if it is indeed a fake token.
8. Remove all fake transfers where contract is a legitimate contract based on results from step 7.
9. Run remove_dup_payouts.py to remove duplicate payouts.
10. payout_legit.py can be used on payouts to filter only those that involved a contract from out legitimate token list.
11. Save csv files as xlsx and perform neccessery manuall operations and statistics.
