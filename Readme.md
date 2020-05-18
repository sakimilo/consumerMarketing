# Consumer Marketing

This is the repository that host the program for generating analytical insights, including visualisation on the data located under `data` folder.

## Getting Started

Make sure Git is installed in your system.

To clone the repository:

```bash
git clone git@github.com:sakimilo/assessment.git
```

Set up environment and install required Python packages

```bash
sh bin/vEnv.sh
```

## Prerequisites

You will need to have a valid Python installation on your system. This has been tested with Python 3.7. It does not assume a particulay version of python, however, it makes no assertions of proper working, either on this version of Python, or on another. 

## Run the program

Make sure your current directory is at the root folder which you can see subfolders as below,
```
.
├── Readme.md
├── bin
├── config
├── data
│   ├── MovieLensDataset
│   │   ├── README.txt
│   │   ├── genome-scores.csv
│   │   ├── genome-tags.csv
│   │   ├── links.csv
│   │   ├── movies.csv
│   │   ├── ratings.csv
│   │   ├── ratings_mini.csv
│   │   └── tags.csv
│   ├── business_vertical.csv
│   ├── camp_data.csv
│   ├── channel_name.csv
│   ├── country.csv
│   ├── intermediate
│   │   ├── jumboData.snappy.parquet
│   │   ├── jumboData_cleansed.snappy.parquet
│   │   └── jumboData_cleansed_engineered.snappy.parquet
├── docs
├── environment.yml
├── notebooks
├── references
├── report
├── resources
├── results
├── sqlScript
└── src
```

To start the program, run below command on bash
```bash
python main.py
```

## Built For

 - Python 3.7

## Contributing

Please send in a pull request.

## Authors

1) Ling Yit - Initial work (October, 2019)