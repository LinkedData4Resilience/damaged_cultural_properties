import TriplyClient from '@triply/triplydb';
import configs, { EtlConfig } from './configs.js';
import readSheets from './read-sheet.js';
import { generateLinkset } from './generate-linkset.js';
import { Store } from 'n3';

async function processSheet<T extends EtlConfig>(config: T) {
  const data = await readSheets(config.inputFile, Object.keys(config.cellProcessors));
  const store = new Store();
  for (const row of data) {
    const iri = config.iriGenerator();
    for (const header in row) {
      const processor = config.cellProcessors[header];
      if (processor.when){
        if (!processor.when(row[header])){
          // skip
          continue;
        }
      }
      store.addQuad(iri, processor.predicate, processor.object(row[header]), config.graph);
    }
  }
  return store;
}

async function run() {

  // db and query name can be adjusted to avoid overwriting exising resources. useful for testing. 
  const dbName = 'cultural-sites-poc'; 
  const queryName = 'link-damage-events-to-cultural-sites'

  // authenticate with triply
  const triply = TriplyClient.get({ token: process.env.TOKEN });
  // handle for the linked4resilience organization
  const account = await triply.getAccount('linked4resilience');
  // handle for a dataset, used only for this projet
  const ds = await account.ensureDataset(dbName, { accessLevel: 'public' });
  // make sure the dataset contains no data (its content is fully automated by this script)
  await ds.clear('graphs');

  
  // add data from both sheets
  await ds.importFromStore(await processSheet(configs.unesco));
  await ds.importFromStore(await processSheet(configs.scienceAtRisk));

  // add earlier data about damage events
  await ds.importFromDataset(await account.getDataset('Integrated-CH-EoR-April-2023'));

  await ds.ensureService('virtuoso', {type:'virtuoso'});

  // refresh services, for updated data to run queries over
  for await (const service of ds.getServices()){
    if (!await service.isUpToDate()) await service.update()
  }

  // generate linkset
  await generateLinkset(account,ds, queryName);
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
