
import { DataFactory, NamedNode, Literal } from 'n3';

const { namedNode, literal } = DataFactory;

function makeSafeAsIriSegment(s: string) {
    s = s.trim().replace(/[^a-zA-Z0-9]/g, '-');
    while (s.includes('--')) {
        s = s.replace('--', '-');
    }
    if (s.startsWith('-')) s = s.slice(1);
    if (s.endsWith('-')) s = s.slice(0, s.length - 1);
    return s.toLowerCase();
}

const datatyped = (dt: NamedNode) => ((val: any) => literal(val, dt));
const langEn = (v: any) => literal(v, 'en');
const langUk = (v: any) => literal(v, 'uk');
const yesnoToBool = (val: any) => literal(val === 'Yes' ? 'true' : 'false', namedNode('http://www.w3.org/2001/XMLSchema#boolean'))
const anyUri = datatyped(namedNode('http://www.w3.org/2001/XMLSchema#anyURI'));

// for padding 0's to a string representation of an integer. 
// for making numeric basenames lexicographically sorted by the number. 
let idCounter = 0;
function pad0(n: number) {
    let str = `${n}`;
    while (str.length < 5) {
        str = `0${str}`;
    }
    return str;
}

type CellProcessor = { predicate: NamedNode, object: ((v: any) => Literal | NamedNode), when?: (v: any) => boolean };
type CellProcessors = { readonly [key: string]: CellProcessor }
export type EtlConfig = {
    iriGenerator: () => NamedNode,
    graph: NamedNode,
    inputFile: string,
    cellProcessors: CellProcessors;
}

const sharedProcessors = {
    "Title of the damage site in English": { predicate: namedNode('https://schema.org/name'), object: langEn },
    "Include or not (Yes/No)": { predicate: namedNode('https://linked4resilience.eu/vocab/include'), object: yesnoToBool },
    "Comment by volunteers": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#comment'), object: langEn },
    "Name of damanged site in Ukrainian on Google Maps": { predicate: namedNode('https://schema.org/alternateName'), object: langUk },
    "Alternative English name on Google Maps": { predicate: namedNode('https://schema.org/alternateName'), object: langEn },
    "Also included in the Wikipedia page?": { predicate: namedNode('https://linked4resilience.eu/vocab/includedInWikipediaPage'), object: yesnoToBool },
    "Note on Wikipedia": { predicate: namedNode('https://linked4resilience.eu/vocab/noteOnWikipedia'), object: langEn },
    "Type of damanged site": { predicate: namedNode('https://linked4resilience.eu/vocab/site-type'), object: (val: string) => namedNode(`https://linked4resilience.eu/data/${makeSafeAsIriSegment(val)}`) },
    "Region": { predicate: namedNode('https://linked4resilience.eu/vocab/region'), object: (val: string) => namedNode(`https://linked4resilience.eu/data/${makeSafeAsIriSegment(val)}`) },
    "Address ": { predicate: namedNode('https://schema.org/address'), object: datatyped(namedNode("https://schema.org/address")) },
    "Geo location": {
        predicate: namedNode(`http://www.opengis.net/ont/geosparql#asWKT`), object: (location: string) => {
            const [y, x] = location.split(',').map(parseFloat);
            return literal(`POINT(${x?.toFixed(10)} ${y?.toFixed(10)})`, namedNode('http://www.opengis.net/ont/geosparql#wktLiteral'));
        }
    },
    "Link to google Maps": { predicate: namedNode(`https://linked4resilience.eu/vocab/googleMaps`), object: anyUri },
    "Wikipedia - English": { predicate: namedNode(`https://linked4resilience.eu/vocab/wikipediaEnglish`), object: anyUri },
    "Wikipedia - Ukrainian": { predicate: namedNode(`https://linked4resilience.eu/vocab/wikipediaUkrainian`), object: anyUri },
    "DBpedia": { predicate: namedNode(`http://www.w3.org/2002/07/owl#sameAs`), object: (v:string)=>{
        return namedNode(v.replace('dbr:','http://dbpedia.org/resource/').trim());
    }, when: (v: string) => v.includes(':') },
    "Reference to the first reported news article, reports, etc.": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#seeAlso'), object: anyUri },
    "Year of construction": { predicate: namedNode(`https://linked4resilience.eu/vocab/constructionYear`), object: datatyped(namedNode('http://www.w3.org/2001/XMLSchema#integer')) },
} as const;

// type foo = typeof ;
sharedProcessors satisfies CellProcessors;

const unesco = {
    iriGenerator: () => DataFactory.namedNode(`https://linked4resilience.eu/data/cultural-sites/${pad0(++idCounter)}`),
    graph: namedNode('https://linked4resilience.eu/graphs/cultural-site-damage-events'),
    inputFile: 'input/unesco.csv',
    cellProcessors: {
        ...sharedProcessors,
        "Date of damage (first reported)": { predicate: namedNode(`https://schema.org/observationTime`), object: datatyped(namedNode('http://www.w3.org/2001/XMLSchema#time')) },
        "Other reporting references": { predicate: namedNode(`http://www.w3.org/2000/01/rdf-schema#seeAlso`), object: anyUri },
    }
} as const;
unesco satisfies EtlConfig;
const scienceAtRisk = {
    iriGenerator: () => DataFactory.namedNode(`https://linked4resilience.eu/data/science-at-risk/${pad0(++idCounter)}`),
    graph: namedNode('https://linked4resilience.eu/graphs/science-at-risk'),
    inputFile: 'input/science-at-risk.csv',
    cellProcessors: {
        ...sharedProcessors,
        "Reference to news articles, reports, etc.": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#seeAlso'), object: anyUri },
        "Reports by Media": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#seeAlso'), object: anyUri },
        "Date of damage": { predicate: namedNode(`https://schema.org/observationTime`), object: datatyped(namedNode('http://www.w3.org/2001/XMLSchema#time')) },
        "Fundraising amount:": { predicate: namedNode(`https://linked4resilience.eu/vocab/fundraisingAmount`), object: datatyped(namedNode("http://www.w3.org/2001/XMLSchema#nonNegativeInteger")) },
        "For what:": { predicate: namedNode(`https://linked4resilience.eu/vocab/purpose`), object: literal },
        "Website": { predicate: namedNode("https://schema.org/url"), object: anyUri },
    }
} as const;
scienceAtRisk satisfies EtlConfig;
export default { unesco, scienceAtRisk };
