## Background
This repo contains files for a data frames benchmark. Currently, the data frame pacakges tested include R's data.table, Python's Polars, DuckDB, Pandas, and Collapse. Others to come. 

All of the packages utilize the installation that comes recommended. For example, DuckDB recommends to install in R as `install.packages("duckdb")` so I utilize that. There are no special installation setup operations taking place for any of the packages. I want to see off the shelf, simple installation, benchmarks. I believe that is what most people use when running these frameworks. Also, I'm using a Windows OS which I believe to be the most popular OS that people use. If anyone wants to run these on MAC or Linux, please share your results and I will display them. Lastly, I'm running this locally, not on cloud, as I also believe that to be the more common usage. Regardless of commonality, I think it's important to see results under these conditions vs. cloud and linux environments only.

The datasets utilized replicates a real world example of a beverage company's data, for 1M, 10M, 100M, and 1B records. The datasets include a Date variable, 4 group variables, and 4 numeric variables. The benchmark tests each dataset, using the Date variables, then adds additional group variables, and then repeats that with additional numeric variables, for each of the datasets.

<br>

## Current Frameworks Tested
* R data.table
* Python Polars
* R DuckDB
* Python Pandas
* R Collapse

<br>

## Current Operations
* Group-By with Sum Aggregation
* Melt Data

<br>

## Replicate Benchmarks

### Aggregation Sum
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run AggSum_datatable.R
* Run AggSum_DuckDB.R
* Run AggSum_Polars.py
* Run AggSum_Pandas.py
* Run AggSum_collapse.py
* Run CombineResults_AggSum
* Done!

</details>

### Melt Data
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run Melt_datatable.R
* Run Melt_DuckDB.R
* Run Melt_Polars.py
* Run Melt_Pandas.py
* Run Melt_collapse.py
* Run CombineResults_Melt
* Done!

</details>

<br>

## Machine Specs
* Windows OS
* Memory: 234GB
* CPU: 32 cores / 64 threads
* AMD Ryzen CPU

<br>

## Benmark Results

In the plots below the x-axis "Experiments" shows four letters with numbers in front of them. This is what they mean:
* M: millions of rows
* N: number of numeric variables
* D: number of date variables
* G: number of additional group variables

<br>

### Sum Aggregation
<details><summary> Click here to see results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1BResults.PNG)

</details>

<br>

### Melt Data
<details><summary> Click here to see results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResultsMelt.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResultsMelt.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResultsMelt_WithDuckDB.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResultsMelt_WithoutDuckDB.PNG)

</details>

