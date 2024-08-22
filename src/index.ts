import TriplyClient from '@triply/triplydb';
import configs, { EtlConfig, regionCache } from './configs.js';
import readSheets from './read-sheet.js';
import { generateLinkset } from './generate-linkset.js';
import { Store } from 'n3';
import getGeonamesExcerpt from './get-geonames-excerpt.js';
import Dataset from '@triply/triplydb/Dataset.js';

async function processSheet<T extends EtlConfig>(config: T) {
  const data = await readSheets(config.inputFile, Object.keys(config.cellProcessors));
  const store = new Store();
  for (const row of data) {
    const iri = config.iriGenerator();
    for (const header in row) {
      const processor = config.cellProcessors[header];
      if ('statements' in processor) {
        store.addQuads(processor.statements(iri, config.graph, row[header]));
      } else {
        if (processor.when) {
          if (!processor.when(row[header])) {
            // skip
            continue;
          }
        }
        const object = await processor.object(row[header]);
        if (!object.value.trim()) continue;
        store.addQuad(iri, processor.predicate, object, config.graph);
      }
    }
  }
  return store;
}

async function run() {

  console.info("Building UNESCO graph");
  const unesco = await processSheet(configs.unesco)
  console.info("Building Science-At-Risk graph");
  const scienceAtRisk = await processSheet(configs.scienceAtRisk)
  console.info("Building Geonames-Excerpt graph");
  const geonames = await getGeonamesExcerpt(regionCache);

  console.info("Connecting to Triplydb.com and setting up environment");

  // db and query name can be adjusted to avoid overwriting exising resources. useful for testing. 
  const dbName = 'linked-4-resilience-2024';
  const queryName = 'link-damage-events-to-cultural-sites-v2'

  // authenticate with triply
  const triply = TriplyClient.get({ token: process.env.TOKEN });
  // handle for the linked4resilience organization
  const account = await triply.getAccount('linked4resilience');
  // handle for a dataset, used only for this projet
  const ds = await account.ensureDataset(dbName, { accessLevel: 'public' });
  // make sure the dataset contains no data (its content is fully automated by this script)
  await ds.clear('graphs');

  console.info("Uploading the graphs we built");
  await ds.importFromStore(unesco);
  await ds.importFromStore(scienceAtRisk);
  await ds.importFromStore(geonames);

  console.info("Importing data from our 2023 project")
  await ds.importFromDataset(await account.getDataset('Integrated-CH-EoR-April-2023'), {
    // we rename the graphs because the 2023 graph names carry no information and are confusing. 
    graphMap: {
      'https://triplydb.com/linked4resilience/Shelters-Kharkiv/graphs/default': 'https://linked4resilience.eu/graphs/kharkiv-shelters-2023',
      'https://triplydb.com/linked4resilience/Integrated-CH-EoR-April-2023/graphs/default': 'https://linked4resilience.eu/graphs/eor-ch-linkset-2023',
      'https://triplydb.com/linked4resilience/Integrated-CH-EoR-April-2023/graphs/default-1': 'https://linked4resilience.eu/graphs/eyes-on-russia-2023',
      'https://triplydb.com/linked4resilience/Integrated-CH-EoR-April-2023/graphs/default-2': 'https://linked4resilience.eu/graphs/civilian-harm-2023',
    }
  });

  console.info("Setting up SPARQL query engine")
  await (await ds.ensureService('virtuoso', { type: 'virtuoso' })).waitUntilRunning();
  await refreshServices(ds);

  console.info("Generate distance-based linkset between new graphs and our 2023 data, using SPARQL");
  await generateLinkset(account, ds, queryName);

  await refreshServices(ds);
}

async function refreshServices(ds: Dataset) {
  // refresh services, for updated data to run queries over
  for await (const service of ds.getServices()) {
    if (!await service.isUpToDate()) await service.update()
  }
}

run().catch(e => {
  console.error(e)
  process.exit(1)
})
process.on('uncaughtException', function (e) {
  console.error('Uncaught exception', e)
  process.exit(1)
})
process.on('unhandledRejection', (reason, p) => {
  console.error('Unhandled Rejection at: Promise', p, 'reason:', reason)
  process.exit(1)
})
