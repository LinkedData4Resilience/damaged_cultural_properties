
import { DataFactory, NamedNode, Literal, Quad } from 'n3';

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
const yesnoToBool = (val: any) => {
    if (!["yes", "no"].includes(val.toLowerCase())) {
        throw new Error(`Unexpected value for bool: "${val}"`);
    }
    return literal(val.toLowerCase() === 'Yes' ? 'true' : 'false', namedNode('http://www.w3.org/2001/XMLSchema#boolean'))
}
const anyUri = datatyped(namedNode('http://www.w3.org/2001/XMLSchema#anyURI'));
const date = (v: string) => {
    if (/^-?\d{4}-\d{2}-\d{2}$/.test(v)) {
        return literal(v, namedNode('http://www.w3.org/2001/XMLSchema#date'));
    }
    if (/^-?\d{4}-\d{2}$/.test(v)) {
        return literal(v, namedNode('http://www.w3.org/2001/XMLSchema#gYearMonth'));
    }
    if (/^-?\d{4}$/.test(v)) {
        return literal(v, namedNode('http://www.w3.org/2001/XMLSchema#gYear'));
    }
    console.warn(`Could not parse date ${v}`);
    return literal(v);
}

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

type CellProcessor = { predicate: NamedNode, object: ((v: any) => Literal | NamedNode), when?: (v: any) => boolean } | {statements: (iri: NamedNode, graph: NamedNode, value: string) => Quad[]};
type CellProcessors = { readonly [key: string]: CellProcessor }
export type EtlConfig = {
    iriGenerator: () => NamedNode,
    graph: NamedNode,
    inputFile: string,
    cellProcessors: CellProcessors;
}

const sharedProcessors = {
    "Title of the damage site in English": { predicate: namedNode('https://schema.org/name'), object: langEn },
    "Comment by volunteers": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#comment'), object: langEn },
    "Name of damanged site in Ukrainian on Google Maps": { predicate: namedNode('https://schema.org/alternateName'), object: langUk },
    "Alternative English name on Google Maps": { predicate: namedNode('https://schema.org/alternateName'), object: langEn },
    "Also included in the Wikipedia page?": { predicate: namedNode('https://linked4resilience.eu/vocab/includedInWikipediaPage'), object: yesnoToBool },
    "Note on Wikipedia": { predicate: namedNode('http://www.w3.org/2000/01/rdf-schema#comment'), object: langEn },
    "Type of damanged site": { predicate: namedNode('https://linked4resilience.eu/vocab/site-type'), object: (val: string) => namedNode(`https://linked4resilience.eu/data/${makeSafeAsIriSegment(val)}`) },
    "Region": { predicate: namedNode('https://linked4resilience.eu/vocab/region'), object: (val: string) => namedNode(`https://linked4resilience.eu/data/${makeSafeAsIriSegment(val)}`) },
    "Address ": { predicate: namedNode('https://schema.org/address'), object: datatyped(namedNode("https://schema.org/address")) },
    "Geo location": {
        statements: (iri: NamedNode, graph: NamedNode, value: string) => {
            const geometry = DataFactory.blankNode();
            const [y, x] = value.split(',').map(parseFloat);
            const wktLiteral = literal(`POINT(${x?.toFixed(10)} ${y?.toFixed(10)})`, namedNode('http://www.opengis.net/ont/geosparql#wktLiteral'));
            return [
                DataFactory.quad(iri, namedNode('http://www.opengis.net/ont/geosparql#hasGeometry'), geometry, graph),
                DataFactory.quad(geometry, namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#'), namedNode('http://www.opengis.net/ont/geosparql#Geometry'), graph),
                DataFactory.quad(geometry, namedNode(`http://www.opengis.net/ont/geosparql#asWKT`), wktLiteral, graph),
            ];
        }
    },
    "Link to google Maps": { predicate: namedNode(`https://linked4resilience.eu/vocab/googleMaps`), object: anyUri },
    "Wikipedia - English": { predicate: namedNode(`https://linked4resilience.eu/vocab/wikipediaEnglish`), object: anyUri },
    "Wikipedia - Ukrainian": { predicate: namedNode(`https://linked4resilience.eu/vocab/wikipediaUkrainian`), object: anyUri },
    "DBpedia": {
        predicate: namedNode(`http://www.w3.org/2002/07/owl#sameAs`), object: (v: string) => {
            return namedNode(v.replace('dbr:', 'http://dbpedia.org/resource/').trim());
        }, when: (v: string) => v.includes(':')
    },
    "Reference to the first reported news article, reports, etc.": { predicate: namedNode('https://linked4resilience.eu/vocab/wasMentionedIn'), object: anyUri },
    "Year of construction": { predicate: namedNode(`https://linked4resilience.eu/vocab/constructionYear`), object: datatyped(namedNode('http://www.w3.org/2001/XMLSchema#gYear')) },
} as const;

sharedProcessors satisfies CellProcessors;

const unesco = {
    iriGenerator: () => DataFactory.namedNode(`https://linked4resilience.eu/data/cultural-sites/${pad0(++idCounter)}`),
    graph: namedNode('https://linked4resilience.eu/graphs/cultural-site-damage-events'),
    inputFile: 'input/unesco.csv',
    cellProcessors: {
        ...sharedProcessors,
        "Date of damage (first reported)": { predicate: namedNode(`https://schema.org/observationTime`), object: date },
        "Other reporting references": { predicate: namedNode(`https://linked4resilience.eu/vocab/wasMentionedIn`), object: anyUri },
    }
} as const;
unesco satisfies EtlConfig;
const scienceAtRisk = {
    iriGenerator: () => DataFactory.namedNode(`https://linked4resilience.eu/data/science-at-risk/${pad0(++idCounter)}`),
    graph: namedNode('https://linked4resilience.eu/graphs/science-at-risk'),
    inputFile: 'input/science-at-risk.csv',
    cellProcessors: {
        ...sharedProcessors,
        "Reference to news articles, reports, etc.": { predicate: namedNode('https://linked4resilience.eu/vocab/wasMentionedIn'), object: anyUri },
        "Reports by Media": { predicate: namedNode('https://linked4resilience.eu/vocab/wasMentionedIn'), object: anyUri },
        "Date of damage": { predicate: namedNode(`https://schema.org/observationTime`), object: date },
        "Fundraising amount:": { predicate: namedNode(`https://linked4resilience.eu/vocab/fundraisingAmount`), object: datatyped(namedNode("http://www.w3.org/2001/XMLSchema#nonNegativeInteger")) },
        "For what:": { predicate: namedNode(`https://linked4resilience.eu/vocab/purpose`), object: literal },
        "Website": { predicate: namedNode("https://schema.org/url"), object: anyUri },
    }
} as const;
scienceAtRisk satisfies EtlConfig;
export default { unesco, scienceAtRisk };
