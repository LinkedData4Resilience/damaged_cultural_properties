# Cultural sites damage events ETL

This repo contains code of an ETL pipeline which extracts data from [the annotated google sheet](https://docs.google.com/spreadsheets/d/1NRfs0vj9GAzC_MqcJeirpMfhqj-Ew7Edu-hHe7HVxs8). 

To use: 
 - clone the repo
 - download the spreadsheet as CSV and place it in the root of the repo as 'annotations.csv'
 - obtain a token from https://triplydb.com/me/-/settings/tokens , with write access
 - `yarn && yarn build && yarn start`

This should update the dataset at https://triplydb.com/linked4resilience/cultural-sites-poc . 

If you change the SPARQL query in `generate-linkset.ts`, then you need to delete https://triplydb.com/linked4resilience/-/queries/link-damage-events-to-cultural-sites for the changes to take effect. 

That dataset consists of 5 graphs, of which two are original. The other three are imported from our earlier work. 

New graphs:
 - [https://linked4resilience.eu/graphs/cultural-site-damage-events](https://triplydb.com/linked4resilience/cultural-sites-poc/table?graph=https%3A%2F%2Flinked4resilience.eu%2Fgraphs%2Fcultural-site-damage-events) : a linked-data version of the annotation spreadsheet
 - [https://linked4resilience.eu/graphs/cultural-sites-linkset](https://triplydb.com/linked4resilience/cultural-sites-poc/table?graph=https%3A%2F%2Flinked4resilience.eu%2Fgraphs%2Fcultural-sites-linkset) : a linkset between the new and old damage events, based on distance. 