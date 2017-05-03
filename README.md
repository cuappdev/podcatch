# Podcatch

Microservice to `catch` podcast information from iTunes on a daily basis

## Required Environment Variables

````bash
COUCHBASE_BASE_URL
# Various buckets' names
PODCASTS_BUCKET
PODCASTS_BUCKET_PASSWORD
NEW_PODCASTS_BUCKET
NEW_PODCASTS_BUCKET_PASSWORD
````

## Development DB Test Data

To populate, ensure you have `virtualenv` running for dependencies.

Test data [`here`](https://www.dropbox.com/s/bg0nrfnfxjp6amc/data.zip?dl=0) (from [`iTunes`](https://itunes.apple.com/us/genre/podcasts/id26?mt=2))

````bash
# If you don't have virutalenv
virtualenv venv
source venv/bin/activate
brew install libcouchbase
sudo -H pip install -r requirements.txt

# Command to run
python dev.py
````

The following queries must be run to establish indexes:

````
CREATE PRIMARY INDEX ON `podcasts`;
````

````
CREATE INDEX `def_pubDate` ON `podcasts`(`pubDate`);
````

````
CREATE INDEX `def_seriesId` ON `podcasts`(`seriesId`);
````

````
CREATE INDEX `def_seriesTitle` ON `podcasts`(`seriesTitle`);
````

````
CREATE INDEX `def_title` ON `podcasts`(`title`);
````

````
CREATE INDEX `def_type` ON `podcasts`(`type`);
````
