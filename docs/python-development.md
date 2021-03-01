# Python Development

## Requirements

This project requires python v3.7

## Linting

This project utilizes:

* black
* flake8

## Development

To do python development against the scripts in this repo, follow these steps:

```
python -m venv .venv
source .venv/bin/activate
wget https://files.pythonhosted.org/packages/80/2f/2c2e674114493e8d6db3dd8da012c287432ee642fc6345c8b25ed51de13b/awsglue-local-1.0.2.tar.gz
pip install ~/awsglue-local-1.0.2.tar.gz
pip install -r requirements.txt
```

Once completed you should be able to run the python scripts:

* scripts/create_csv.py
* scripts/loadpartition.py
