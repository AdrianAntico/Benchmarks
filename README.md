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
* Melt
* Cast
* Lags

<br>

## Dataset Attributes
Common attributes across datasets:
> Brand: 13 levels
> Category: 6 levels
> Beverage Flavor: 21 levels
> Four numeric variables
> One Date Variable

1. 1M Rows Data
  * Customer: 99 levels
2. 10M Rows Data
  * Customer: 1071 levels
3. 100M Rows Data
  * Customer: 10793 levels
4. 1Bn Rows Data
  * Customer: 108017 levels

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

### Melt
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

### Cast
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run Cast_datatable.R
* Run Cast_DuckDB.R
* Run Cast_Polars.py
* Run Cast_Pandas.py
* Run Cast_collapse.py
* Run CombineResults_Cast
* Done!

</details>

### Lags
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run Lags_datatable.R
* Run Lags_DuckDB.R
* Run Lags_Polars.py
* Run Lags_Pandas.py
* Run Lags_collapse.py
* Run CombineResults_Lags
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

#### Total Run Time (excluding 1bn rows run times)
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/AggSum_TotalRunTime.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults.PNG)

#### Top 3 Performers
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResultsTop3.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults.PNG)

#### Top 3 Performers
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResultsTop3.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1BResults.PNG)

</details>

<br>

### Melt
<details><summary> Click here to see results </summary>

#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Melt_TotalRunTime.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Melt.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Melt.PNG)

<br>

##### With DuckDB: Note - DuckDB timed out after a few successful runs

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Melt_WithDuckDB.PNG)

<br>

##### Without DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Melt_WithoutDuckDB.PNG)

</details>

<br>

### Cast
<details><summary> Click here to see results </summary>

#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Cast_TotalRunTime.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Cast.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Cast.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Cast.PNG)

</details>

<br>

### Lags
<details><summary> Click here to see results </summary>

#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Lags_TotalRunTime.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Lags.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Lags.PNG)

<br>

##### With DuckDB: Note - DuckDB timed out after a few successful runs

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Lags_WithDuckDB.PNG)

<br>

##### Without DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Lags_WithoutDuckDB.PNG)

</details>

<br>
