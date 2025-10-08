# misinfo-dataset

## Setup

### Ensure the following are setup:

- Python version 3.12.11

Ensure dependencies are installed. (via `nix.shell` or `requirements.txt`)

- pandas
- rich

## Adding new data in `/raw` and combining data into a single large dataset

After adding new data in `/raw` directory, run the following to process the data into two columns `claim;label`, by running the following script which will bring up a TUI:

```bash
python index.py
```

After adding new data, combine it into one large dataset:

```bash
python scripts/combine.py
```

## Raw datasets sources:

- [Ecstra/factum](https://huggingface.co/datasets/Ecstra/factum/tree/main)
- [GonzaloA/fake_news](https://huggingface.co/datasets/GonzaloA/fake_news/blob/main/train.csv)
- [roupenminassian/twitter-misinformation](https://huggingface.co/datasets/roupenminassian/twitter-misinformation/blob/main/training_data.csv)
