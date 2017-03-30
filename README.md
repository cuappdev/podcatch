# Podcatch

Microservice to `catch` podcast information from iTunes on a daily basis

## Required Environment Variables

````bash
COUCHBASE_URL
COUCHBASE_PASSWORD
````

## Development DB Test Data

To populate, ensure you have `virtualenv` running for dependencies.

Test data [`here`](https://www.dropbox.com/s/bg0nrfnfxjp6amc/data.zip?dl=0) (from [`iTunes`](https://itunes.apple.com/us/genre/podcasts/id26?mt=2))

````bash
# If you don't have virutalenv
virtualenv venv
source venv/bin/activate
brew install libcouchbase
sudo pip install -r requirements.txt

# Command to run
python dev.py
````
