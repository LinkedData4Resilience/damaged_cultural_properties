
import fs from 'fs-extra';
import csv from 'csv-parser';

export default async function <T extends { [key: string]: any }>(csvPath: string, expectedHeaders: string[]) {

    return new Promise<Array<T>>((resolve, reject) => {
        const rows: Array<T> = [];
        const keysFound: string[] = [];
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                for (const key in row) {
                    if (key === '') {
                        // index column
                        continue;
                    }
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

