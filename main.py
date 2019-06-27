# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# +
from gssutils import *
from requests import Session
from cachecontrol import CacheControl
from cachecontrol.caches.file_cache import FileCache
from cachecontrol.heuristics import ExpiresAfter

scraper = Scraper('https://statswales.gov.wales/Catalogue/Housing/Social-Housing-Vacancies/'
                  'vacancies-by-area-availability-duration',
                  session=CacheControl(Session(),
                                       cache=FileCache('.cache'),
                                       heuristic=ExpiresAfter(days=7)))
scraper
# -

if len(scraper.distributions) == 0:
    from gssutils.metadata import Distribution
    dist = Distribution(scraper)
    dist.title = 'Dataset'
    dist.downloadURL = 'http://open.statswales.gov.wales/dataset/hous1401'
    dist.mediaType = 'application/json'
    scraper.distributions.append(dist)
table = scraper.distribution(title='Dataset').as_pandas()
table

table.columns

# StatsWales uses labels (ItemName_ENG) and notations (Code) for concepts, as well as alternative notations (AltCodeN) where appropriate. We'll use these codes for concepts specific to StatsWales, and for others we'll try to harmonise via labels.

cols = {
    'Area_AltCode1': 'Geography',
    'Availability_Code': 'Availability',
    'Data': 'Value',
    'Duration_Code': 'Vacancy length', # existing component
    'Provider_Code': 'Provider',
    'Vacancy_Code': 'Vacancy type',
    'Year_Code': 'Period'
}
to_remove = set(table.columns) - set(cols.keys())
table.rename(columns=cols, inplace=True)
table.drop(columns=to_remove, inplace=True)
table

# The OData API offers an "Items" endpoint that enumerates the values of the various dimensions and provides information about the hierarchy.

try:
    items_dist = scraper.distribution(title='Items')
except:
    from gssutils.metadata import Distribution
    dist = Distribution(scraper)
    dist.title = 'Items'
    dist.downloadURL = 'http://open.statswales.gov.wales/en-gb/discover/datasetdimensionitems?$filter=Dataset%20eq%20%27hous1401%27'
    dist.mediaType = 'application/json'
    scraper.distributions.append(dist)
    items_dist = scraper.distribution(title='Items')
items = items_dist.as_pandas()
items

# +
from collections import OrderedDict
item_cols = OrderedDict([
    ('Description_ENG', 'Label'),
    ('Code', 'Notation'),
    ('Hierarchy', 'Parent Notation'),
    ('SortOrder', 'Sort Priority')
])

def extract_codelist(dimension):
    codelist = items[items['DimensionName_ENG'] == dimension].rename(
        columns=item_cols).drop(
        columns=set(items.columns) - set(item_cols.keys()))[list(item_cols.values())]
    codelist['Notation'] = codelist['Notation'].map(
        lambda x: str(int(x)) if str(x).endswith(".0") else str(x)
    )
    return codelist

codelists = {
    'vacancies': extract_codelist('Vacancy'),
    'providers': extract_codelist('Provider'),
    'availability': extract_codelist('Availability'),
    'durations': extract_codelist('Duration')
}

out = Path('out')
out.mkdir(exist_ok=True, parents=True)

for name, codelist in codelists.items():
    codelist.to_csv(out / f'{name}.csv', index = False)
    display(name)
    display(codelist)
# -

table['Period'] = table['Period'].map(lambda x: f'gregorian-interval/{str(x)[:4]}-03-31T00:00:00/P1Y')
table['Vacancy type'] = table['Vacancy type'].map(lambda x: str(int(x)) if str(x).endswith(".0") else str(x))
table['Vacancy length'] = table['Vacancy length'].map({
    1: 'less-than-6-months',
    2: '6-months-or-more',
    3: 'total'}.get)
table['Measure Type'] = 'Count'
table['Unit'] = 'vacancies'


table.drop_duplicates().to_csv(out / 'observations.csv', index = False)

schema = CSVWMetadata('https://ons-opendata.github.io/ref_housing/')
schema.create(out / 'observations.csv', out / 'observations.csv-schema.json')

from datetime import datetime
scraper.dataset.family = 'housing'
scraper.dataset.theme = THEME['housing-planning-local-services']
scraper.dataset.modified = datetime.now()
scraper.dataset.creator = scraper.dataset.publisher
with open(out / 'dataset.trig', 'wb') as metadata:
    metadata.write(scraper.generate_trig())

table


