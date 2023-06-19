# MQTransport

Message queue based interface for communications in event-driven systems

## Install

```bash
git clone https://github.com/Bondifuzz/mqtransport
pip install mqtransport
```

## Documentation

See `docs` folder

## Examples

See `examples` folder

## Contributing

### Prepare repository

Run commands below to get sources and run message broker:

```bash
git clone https://github.com/Bondifuzz/mqtransport
cd mqtransport

pip install -r requirements-sqs.txt
pip install -r requirements-dev.txt
pip install -r requirements-test.txt

ln -s local/dotenv .env
ln -s local/docker-compose.yml docker-compose.yaml
ln -s local/elasticmq.conf elasticmq.conf

docker-compose -p mqtransport up -d
```

### Code documentation

Generate code documentation:

```bash
pdoc3 --http 127.0.0.1:8080 ./mqtransport
```

### Running tests

```bash
# run unit tests
pytest -vv mqtransport/tests/unit

# run functional tests
pytest -vv mqtransport/tests/functional
```

### Spell checking

Download VSCode extensions:
- streetsidesoftware.code-spell-checker
- streetsidesoftware.code-spell-checker-russian

Download `cspell` and run to check spell in all sources

```bash
sudo apt install nodejs npm
sudo npm install -g cspell
sudo npm install -g @cspell/dict-ru_ru
cspell link add @cspell/dict-ru_ru
cspell "**/*.{py,md,txt}"
```