
import fs from 'fs-extra';
import csv from 'csv-parser';

export default async function <T extends { [key: string]: any }>(csvPath: string, expectedHeaders: string[]) {

    return new Promise<Array<T>>((resolve, reject) => {
        const rows: Array<T> = [];
        const keysFound: string[] = [];
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                if (row[''].includes("in the")) return;
                delete row[''];

                const shouldInclude = row["Include or not (Yes/No)"];
                if (!["Yes", "No"].includes(shouldInclude)) {
                    console.warn(row)
                    console.warn(`Unexpected value for bool: "${shouldInclude}"`);
                    return;
                }
                if (shouldInclude === 'No'){
                    return;
                }
                delete row["Include or not (Yes/No)"];
                for (const key in row) {
                    if (!expectedHeaders.includes(key)) {
                        return reject(`Got unexpected key ${key} in ${csvPath}`);
                    }
                    if (!keysFound.includes(key)) {
                        keysFound.push(key);
                    }
                }
                delete row['']
                rows.push(row);
            })
            .on('end', () => {
                {
                    for (const key of keysFound){
                        if (!expectedHeaders.includes(key)){
                            return reject("Got unexpected header: " + key);
                        }
                    }
                    resolve(rows)
                }
            });
    });
}

