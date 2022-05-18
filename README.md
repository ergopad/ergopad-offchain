# ergopad-offchain

## Requirements

Running the ergo off chain execution setup requires [Docker](https://docs.docker.com/get-started/), [Docker compose v2](https://docs.docker.com/compose/compose-file/compose-file-v2/) and [ergo-offchain-execution](https://github.com/ergo-pad/ergo-offchain-execution) running

## Installation

Clone the repo
```sh
git clone https://github.com/ergo-pad/ergo-offchain-execution
```

Run/build the bots using the following command:
```sh
cd ergo-offchain-execution
sudo docker-compose up -d --build
```

Ensure the services are running correct by checking their logs.
