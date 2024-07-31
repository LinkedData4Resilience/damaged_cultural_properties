
import fs from 'fs-extra';
import csv from 'csv-parser';
import { DataFactory, NamedNode, Store, Literal } from 'n3';
import Dataset from '@triply/triplydb/Dataset.js';

const { quad, namedNode, literal } = DataFactory;

function makeSafeAsIriSegment(s: string) {
    s = s.trim().replace(/[^a-zA-Z0-9]/g, '-');
    while (s.includes('--')) {
        s = s.replace('--', '-');
    }
    if (s.startsWith('-')) s = s.slice(1);
    if (s.endsWith('-')) s = s.slice(0, s.length - 1);
    return s.toLowerCase();
}

// used for padding 0's to a string representation of an integer. 
// useful for making numeric basenames lexicographically sorted by the number. 
function pad0(n: number) {
    let str = `${n}`;
    while (str.length < 5) {
        str = `0${str}`;
    }
    return str;
}


export async function processAnnotationsSheet(ds: Dataset) {

    const xsdBool = namedNode('http://www.w3.org/2001/XMLSchema#boolean');
    const xsdAnyUri = namedNode('http://www.w3.org/2001/XMLSchema#anyURI');
    const xsdInt = namedNode('http://www.w3.org/2001/XMLSchema#integer');
    const xsdTime = namedNode('http://www.w3.org/2001/XMLSchema#time');

    let idCounter = 0;

    const store = new Store();

    

    await new Promise<void>((resolve, reject) => {
        fs.createReadStream('annotations.csv')
            .pipe(csv())
            .on('data', (row) => {
                const csIri = namedNode(`https://linked4resilience.eu/data/cultural-sites/${pad0(++idCounter)}`);

                function addQuad(predicate: NamedNode, object: NamedNode|Literal){
                    store.addQuad(quad(csIri, predicate, object, namedNode('https://linked4resilience.eu/graphs/cultural-site-damage-events')))
                }

                {
                    const title = row['Title of the damage site in English'];
                    if (title) {
                        
                        addQuad(namedNode('https://schema.org/name'), literal(title, 'en'));
                    }
                }

                {
                    const include = row['Include or not (Yes/No)'];
                    if (include) {
                        const val = include === 'Yes' ? 'true' : 'false';
                        addQuad(namedNode('https://linked4resilience.eu/vocab/include'), literal(val, xsdBool));
                    }
                }

                {
                    const comment = row['Comment by volunteers'];
                    if (comment) {
                        addQuad(namedNode('http://www.w3.org/2000/01/rdf-schema#comment'), literal(comment, 'en'));
                    }
                }

                {
                    const altNameUkrainian = row['Name of damanged site in Ukrainian on Google Maps'];
                    if (altNameUkrainian) {
                        addQuad(namedNode('https://schema.org/alternateName'), literal(altNameUkrainian, 'uk'));
                    }
                }

                {
                    const altNameEnglish = row['Alternative English name on Google Maps'];
                    if (altNameEnglish) {
                        addQuad(namedNode('https://schema.org/alternateName'), literal(altNameEnglish, 'en'));
                    }
                }

                {
                    const includedInWikipediaPage = row['Also included in the Wikipedia page?'];
                    if (includedInWikipediaPage) {
                        const val = includedInWikipediaPage === 'Yes' ? 'true' : 'false';
                        addQuad(namedNode('https://linked4resilience.eu/vocab/includedInWikipediaPage'), literal(val, xsdBool));
                    }
                }


                {
                    const noteOnWikipedia = row['Note on Wikipedia'];
                    if (noteOnWikipedia) {
                        addQuad(namedNode('https://linked4resilience.eu/vocab/noteOnWikipedia'), literal(noteOnWikipedia, 'en'));
                    }
                }

                {
                    const type = row['Type of damanged site'];
                    if (type) {
                        // todo use dbpedia or maybe schema.org instead
                        const typeIri = `https://linked4resilience.eu/data/${makeSafeAsIriSegment(type)}`;
                        addQuad(namedNode('https://linked4resilience.eu/vocab/site-type'), namedNode(typeIri));
                    }
                }

                {
                    const region = row['Region'];
                    if (region) {
                        // todo use geodata instead
                        const regionIri = `https://linked4resilience.eu/data/${makeSafeAsIriSegment(region)}`;
                        addQuad(namedNode('https://linked4resilience.eu/vocab/region'), namedNode(regionIri));
                    }
                }

                {
                    const address = row['Address'];
                    if (address) {
                        // todo use geodata instead
                        addQuad(namedNode('https://schema.org/address'), literal("https://schema.org/address"));
                    }
                }
                function problem(s: string) {
                    console.warn(s);
                }
                {
                    const location = row['Geo location'];
                    if (location) {
                        const [y, x] = location.split(',').map(parseFloat);
                        if (isNaN(y) || isNaN(x) || !y || !x){
                            problem(`Problematic coordinate: (${x}, ${y})\n  Based on string "${location}"\n  IRI: ${csIri.value}`);
                        }
                        if (x < 22.0856083513) {
                            problem(`Location is west of Ukraine: ${location}`)
                        }
                        if (x > 44.3614785833) {
                            problem(`Location is east of Ukraine: ${location}`)
                        }
                        if (y < 40.0807890155) {
                            problem(`Location is south of Ukraine: ${location}`)
                        }
                        if (y > 52.3350745713) {
                            problem(`Location is north of Ukraine: ${location}`)
                        }
                        if (x && y){
                            addQuad(namedNode(`http://www.opengis.net/ont/geosparql#asWKT`), literal(`POINT(${x.toFixed(10)} ${y.toFixed(10)})`, namedNode('http://www.opengis.net/ont/geosparql#wktLiteral')))
                        }
                    }
                }

                {
                    const gmaps = row['Link to google Maps'];
                    if (gmaps) {
                        addQuad(namedNode(`https://linked4resilience.eu/vocab/googleMaps`),
                            literal(gmaps, xsdAnyUri)
                        );
                    }
                }

                {
                    const wikipediaEnglish = row['Wikipedia - English'];
                    if (wikipediaEnglish) {
                        addQuad(namedNode(`https://linked4resilience.eu/vocab/wikipediaEnglish`),
                            literal(wikipediaEnglish, xsdAnyUri)
                        );
                    }
                }

                {
                    const wikipediaUkrainian = row['Wikipedia - Ukrainian'];
                    if (wikipediaUkrainian) {
                        addQuad(namedNode(`https://linked4resilience.eu/vocab/wikipediaUkrainian`),
                            literal(wikipediaUkrainian, xsdAnyUri)
                        );
                    }
                }

                {
                    const dbpedia = row['Dbpedia'];
                    if (dbpedia) {
                        addQuad(namedNode(`http://www.w3.org/2002/07/owl#sameAs`),
                            namedNode(dbpedia)
                        );
                    }
                }

                {
                    const reference = row['Reference to news articles, reports, etc.'];
                    if (reference) {
                        addQuad(namedNode(`http://www.w3.org/2000/01/rdf-schema#seeAlso`),
                            literal(reference, xsdAnyUri)
                        );
                    }
                }
                {

                    {
                        const constructionYear = row['Year of construction'];
                        if (constructionYear) {
                            addQuad(namedNode(`https://linked4resilience.eu/vocab/constructionYear`),
                                literal(constructionYear, xsdInt)
                            );
                        }
                    }

                    {
                        // note: using same iri as for Reference, 2 above
                        const news = row['Reports by Media'];
                        if (news) {
                            addQuad(namedNode(`http://www.w3.org/2000/01/rdf-schema#seeAlso`),
                                literal(news, xsdAnyUri)
                            );
                        }
                    }

                    {
                        const news = row['Date of damage'];
                        if (news) {
                            addQuad(namedNode(`https://schema.org/observationTime`),
                                literal(news, xsdTime)
                            );
                        }
                    }
                }
            })
            .on('end', () => { resolve() });
    });
    await ds.importFromStore(store);
}

