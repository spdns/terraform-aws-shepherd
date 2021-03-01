# Python Development

## Requirements

This project requires python v3.7. If you are using `pyenv` then the `.python-version` file will automatically switch
your development environment to the correct version for you.

## Linting

This project utilizes:

* black - formatting
* flake8 - linting

## Development

To do python development against the scripts in this repo, follow these steps:

```
python -m venv .venv
source .venv/bin/activate
wget https://files.pythonhosted.org/packages/80/2f/2c2e674114493e8d6db3dd8da012c287432ee642fc6345c8b25ed51de13b/awsglue-local-1.0.2.tar.gz
pip install -U pip
pip install ~/awsglue-local-1.0.2.tar.gz
pip install -r requirements.txt
```

## Env Vars

Install the tool [Direnv](https://direnv.net) and then run `direnv allow`. Next, modify `.envrc.local` file and add these lines:

```sh
# Use the glue role to ensure you run the script with the same permissions as the glue job
export AWS_ROLE_ARN=arn:aws-us-gov:iam::<ACCOUNT_ID>:role/shepherd-global-glue-role
export SALT="SOMESALTVALUE"
```

Modify the values to be appropriate for your testing. Now run `direnv allow` again.

## Testing

Once you have completed all the setup steps you should be able to run the python scripts:

* scripts/create_csv.py
* scripts/loadpartition.py

