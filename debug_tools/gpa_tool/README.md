# GPA tool

## Script Description

This script interacts with the Metaplex Core program on the SVM blockchains to retrieve assets associated with a specific collection and process a CSV file containing hexadecimal keys. It converts these keys to Base58 and identifies keys that are missing from the `assets_v3.csv` file. The result is saved to a new CSV file.

---

## Prerequisites

### Node.js and npm
Ensure you have **Node.js** installed (v14 or higher recommended).

### Install Dependencies
Run the following command to install the required packages:

```bash
npm install @metaplex-foundation/umi-bundle-defaults @metaplex-foundation/mpl-core @metaplex-foundation/umi bs58 fs
```

### Set Up a SVM RPC Node
The script uses the RPC endpoint `https://api.mainnet-beta.solana.com`. You can replace this with your preferred RPC endpoint if needed.

---

## How to Use

1. **Set Up the Collection Public Key**
   Replace the placeholder public key `C8uYT2W93pBcmMVxSoUyzTW5mKVFTEpMNEPx1Y15MFyk` with the public key of the Core collection you want to query.

2. **Prepare the CSV File**
   - Create a CSV file named `assets_v3.csv` in the root directory.
        - this file can be created by selecting keys from the PG. Here is SQL request to select asset keys from collection pointed above. `select ast_pubkey from assets_v3 where ast_collection = decode('a57708125d64ff943f1adf2fa45bfb7c0d8e581d6f3d036d6e41d64cd70434f3', 'HEX');`
   - The file must include a header row with `ast_pubkey` as the column name.
   - Each subsequent row should contain hexadecimal-encoded keys (e.g., `0x1234abcd`).

3. **Run the Script**
   Execute the script with:

   ```bash
   ts-node app.ts
   ```

4. **Output**
   - The number of assets in the collection is printed to the console.
   - Missing keys (those not found in the PG data) are written to `absentKeys.csv`.

---

## Functionality Overview

### `main`
1. Connects to the SVM RPC.
2. Fetches all assets associated with the specified collection key.
3. Extracts public keys of the assets.

### `processCsvToBase58`
1. Reads the `assets_v3.csv` file.
2. Validates the file format.
3. Converts hexadecimal keys from the file to Base58.
4. Compares these keys with the blockchain data.
5. Writes absent keys to `absentKeys.csv`.

---

## Example

### Input: `assets_v3.csv`

```csv
ast_pubkey
0x1234abcd
0x5678efgh
```

### Output: `absentKeys.csv`

```csv
base58_key
3QJmV3qfvL9SuYo34YihAfMZhD2xBn84cvTL9W5ddWKH
```

---

## Notes
- Update the RPC endpoint and collection key as needed.
- Ensure you have sufficient permissions to read/write files in the directory. 

Feel free to extend the script for additional functionalities or integrate it into a larger SVM application.