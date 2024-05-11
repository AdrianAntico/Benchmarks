This repo contains files for a data frames benchmark. Currently, the data frame pacakges tested include R's data.table, Python's Polars, and DuckDB. Others to come. 

There are no special installation setup operations taking place for any of the packages. I want to see off the shelf, simple installation, benchmarks. 

The data utilized replicates a real world example of a beverage company's data, for 1M, 10M, and 100M records. There is a Date variables, 4 group variables, and 4 numeric variables. The benchmark tests each dataset, using the Date variables, then adds additional group variables, and then repeats that with additional numeric variables, for each of the datasets.

Currently, I've built out group by operations but I plan on adding additional operations over time. Pull requests are welcome and so are issues and ideas!

## Replicate Benchmarks
1. Fork the repo and clone it to your local machine
2. Modify the Path variable at the top of each script to reflect your file location
3. Run FakeBevDataBuilds.R to generate the benchmarking datasets
4. Run BenchmarkingDatatable.R
5. Run BenchmarkingDuckDB.R
6. Run BenchmarkingPolars.py
7. Run BenchmarkCombineResults
8. Done!

<br>

## Machine Specs
* Memory: 256GB
* CPU: 32 cores / 64 threads

<br>

## Benmark Results

In the plots below the x-axis "Experiments" shows four letters with numbers in front of them. This is what they mean:
* M: millions of rows
* N: number of numeric variables
* D: number of date variables
* G: number of additional group variables

<br>

### Sum Aggregation
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults.PNG)

