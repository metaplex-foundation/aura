import { createUmi } from '@metaplex-foundation/umi-bundle-defaults'
import {  
    mplCore, 
    getAssetV1GpaBuilder, 
    Key, 
    updateAuthority, 
} from '@metaplex-foundation/mpl-core'
import { generateSigner, publicKey, signerIdentity } from '@metaplex-foundation/umi';
import * as fs from 'fs';
import bs58 from 'bs58';

const umi = createUmi('https://api.mainnet-beta.solana.com', 'processed').use(mplCore())
const payer = generateSigner(umi);

umi.use(signerIdentity(payer));

async function main() {
    const collectionKey = publicKey("C8uYT2W93pBcmMVxSoUyzTW5mKVFTEpMNEPx1Y15MFyk");

    const assetsByCollection = await getAssetV1GpaBuilder(umi)
        .whereField('key', Key.AssetV1)
        .whereField(
            'updateAuthority',
            updateAuthority('Collection', [collectionKey])
        )
        .getDeserialized();

    const publicKeyMap: string[] = [];

    for (const element of assetsByCollection) {
        publicKeyMap.push(element.publicKey.toString());
    }

    console.log(assetsByCollection.length);

    await processCsvToBase58("./assets_v3.csv", publicKeyMap);

}

async function processCsvToBase58(filePath: string, keysFromTheNetwork: string[]): Promise<void> {
    try {
        if (!fs.existsSync(filePath)) {
            console.error('File does not exist:', filePath);
            return;
        }

        const csvData = fs.readFileSync(filePath, 'utf-8');
        const rows = csvData.split('\n').filter(row => row.trim() !== '');

        const header = rows[0].trim();
        if (header !== 'ast_pubkey') {
            console.error('Invalid CSV format. Expected header: "ast_pubkey".');
            return;
        }

        const keyFromTheDB: string[] = [];

        const hexValues = rows.slice(1);
        hexValues.forEach((hex, index) => {
            try {
                let trimmedHex = hex.trim();

                if (trimmedHex.startsWith('0x')) {
                    trimmedHex = trimmedHex.slice(2);
                }

                if (!/^([0-9A-Fa-f]+)$/.test(trimmedHex)) {
                    console.warn(`Invalid HEX value at row ${index + 2}:`, hex);
                    return;
                }

                const buffer = Buffer.from(trimmedHex, 'hex');
                const base58 = bs58.encode(buffer);
                keyFromTheDB.push(base58);
            } catch (error) {
                console.error(`Error processing row ${index + 2}:`, error);
            }
        });

        const absentKeys = keysFromTheNetwork.filter(key => !keyFromTheDB.includes(key));

        const h = 'base58_key\n';

        const r = absentKeys.map(key => key).join('\n');

        const csvContent = h + r;

        fs.writeFileSync("./absentKeys.csv", csvContent, 'utf-8');
    } catch (error) {
        console.error('Error reading or processing the file:', error);
    }
}


main().catch(console.error);
