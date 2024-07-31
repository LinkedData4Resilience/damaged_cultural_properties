import TriplyClient from '@triply/triplydb';
import { processAnnotationsSheet } from './process-annotations-sheet.js';
import { generateLinkset } from './generate-linkset.js';


async function run() {
  // authenticate with triply
  const triply = TriplyClient.get({ token: process.env.TOKEN });
  // handle for the linked4resilience organization
  const account = await triply.getAccount('linked4resilience');
  // handle for a dataset, used only for this projet
  const ds = await account.ensureDataset('cultural-sites-poc', { accessLevel: 'public' });
  // make sure the dataset contains no data (its content is fully automated by this script)
  await ds.clear('graphs');

  // add data from the annotations sheet
  await processAnnotationsSheet(ds);
  // add earlier data about damage events
  await ds.importFromDataset(await account.getDataset('Integrated-CH-EoR-April-2023'));
  for await (const service of ds.getServices()){
    if (!await service.isUpToDate()) await service.update()
  }
  // generate linkset
  await generateLinkset(account,ds);
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
