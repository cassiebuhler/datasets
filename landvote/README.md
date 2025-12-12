# README

The raw Landvote data only lists the location of the measure and doesn't have a geometry column. For that reason, I join the landvote data with census spatial data (state, county, subdivision, and places) so we can map Landvote measures. In addition, I add political party affiliation to each measure, where each jurisdiction's political party is recorded. Details on methods are in `add_political_parties.ipynb`. 

Summary: To process Landvote, I add spatial and political party data. 

These are the steps:
1. Download/clean Landvote data
2. Join with census polygon data (state, county, subdivision, and places)
3. Join with political parties

Steps 1-2 are in `preprocess_landvote.ipynb` and step 3 is in `add_political_parties.ipynb`.

I also hexed the data at zoom 8. To run that, execute this in a terminal

``python landvote_hex.py``