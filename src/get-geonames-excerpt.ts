import axios from 'axios';
import { DataFactory, NamedNode, Store } from 'n3';
import { RdfXmlParser } from 'rdfxml-streaming-parser';

// Function to download the RDF file
async function downloadRDF(url: string): Promise<string> {
    while (true){
        try {
            const response = await axios.get(url);
            return response.data;
        } catch (error: any) {
            // probably will succeed on retry
            await new Promise<void>((resolve, reject)=>{
                const timeout = setTimeout(()=>{
                    clearTimeout(timeout);
                    resolve();
                }, 1000)
            })
        }
    }
}

function parseRDFXMLStringToStore(rdfXmlString: string, store:Store) {
    const parser = new RdfXmlParser();
    const promise = new Promise<void>((resolve, reject) => {
        parser.on('data', (quad) => {
            store.addQuad(DataFactory.quad(quad.subject, quad.predicate, quad.object, DataFactory.namedNode('https://linked4resilience.eu/graphs/geonames-excerpt')));
        });
        parser.on('end', () => {
            resolve();
        });
        parser.on('error', (error) => {
            reject(error);
        });
    });
    for (const part of rdfXmlString.split('\n')){
        if (part.trim().length){
            parser.write(part);
        }
    }
    parser.end();
    return promise;
}


async function getRdfForGeonamesIntegerId(s:string, store:Store){
    const rdfUrl = `https://www.geonames.org/${s}/about.rdf`;
    const rdfData = await downloadRDF(rdfUrl);
    return parseRDFXMLStringToStore(rdfData, store);
}

export default async function (regions: { [key: string]: NamedNode }) {
    const store = new Store();
    for (const region of Object.values(regions)) {
        const id = region.value.split('/').filter(v => !!v.length).pop()!;
        const promise = getRdfForGeonamesIntegerId(id, store);
        await promise;
    }
    await getRdfForGeonamesIntegerId('690791',store) // ukraine country
    return store;
} 