import { Account } from "@triply/triplydb/Account.js";
import Dataset from "@triply/triplydb/Dataset.js";
import { Parser, Quad, Store, DataFactory } from "n3";
const {namedNode, quad} = DataFactory;

/**
 * SPARQL query used to link our old 2023-data to our new 2024-data about damage to cultural sites
 */
const linksetConstructionQuery = `
prefix geof: <http://www.opengis.net/def/function/geosparql/>
prefix geo: <http://www.opengis.net/ont/geosparql#>
prefix sdo: <https://schema.org/>
prefix xsd: <http://www.w3.org/2001/XMLSchema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
construct {
  ?damageEvent <https://linked4resilience.eu/vocab/isCloseInLocationTo> ?cultureSiteDamage .
} where {

  # graph pattern for a wkt literal of a cultural site damage event (newest data, july 2024)
  ?cultureSiteDamage geo:hasGeometry ?geo . 
  ?geo geo:asWKT ?wkt1 .
  
  # graph pattern for damage events from previous work (2023)
  ?damageEvent a <http://semanticweb.cs.vu.nl/2009/11/sem/Event>.
  ?damageEvent sdo:location ?location .
  ?location sdo:geo ?geo2 .
  ?geo2 sdo:latitude ?latitude .
  ?geo2 sdo:longitude ?longitude .
  # construct a WKT literal from its geodata.   
  bind(
    strdt( concat('Point (',str(xsd:decimal(?longitude)),' ',str( xsd:decimal(?latitude)),')') , geo:wktLiteral)
    as ?wkt2
  )

  # compute the distance between the two points
  bind (geof:distance(?wkt1, ?wkt2, <http://www.opengis.net/def/uom/OGC/1.0/meter>) as ?dist)

  # emit a row if the two events are very close (<100 meters)
  # (this means we assume the two events are the same)
  # (TODO consider also filtering by date)
  filter(?dist < 100) 
}` as const;

export async function generateLinkset(account: Account, ds: Dataset, queryName:string) {

    try {
        await account.getQuery(queryName);
    } catch (_) {
        // probably doesn't exist yet
        await account.addQuery(queryName,
            { 'dataset': ds, serviceType: 'virtuoso', queryString: linksetConstructionQuery , accessLevel: 'public'})
    }
    const query = await account.getQuery(queryName);

    const queryApi = await query.getRunLink()

    // if the page size is not very small, we'll get interrupted by timeouts!
    let pageSize = 3;
    let page = 1;
    const store = new Store();
    let retry = 0
    while (true) {
        const url = `${queryApi}?pageSize=${pageSize}&page=${page}`
        const response = await fetch(url, { headers: { Authorization: `Bearer ${process.env['TOKEN']}` } });
        const result = await response.text();
        if (result.includes('{"message":"Query has timed out."}')){
            pageSize = 1;
            retry++;
            console.log('retry',retry)
            continue;
        }
        retry = 0;
        pageSize++;
        const parser = new Parser();
        console.log(url)
        console.log(result)
        let quads = await new Promise<Quad[]>((resolve, reject) => {
            const quads: Quad[] = []
            parser.parse(result,
                (error, quad) => {
                    if (error) return reject(error);
                    if (quad) quads.push(quad)
                    else return resolve(quads)
                });
        })
        if (quads.length){
            quads = quads.map(q=>{
                return quad(q.subject,q.predicate,q.object,namedNode('https://linked4resilience.eu/graphs/cultural-sites-linkset'))
            })
            store.addQuads(
                quads
            );
        } else {
            break;
        }
        page++;
    }
    await ds.importFromStore(store);
}