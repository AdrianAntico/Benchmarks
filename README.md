<p align="center">
  <img src="https://raw.githubusercontent.com/AdrianAntico/Benchmarks/main/logo.PNG" width="1000">
</p>



Last Updated: 
Aggregation: 2025-11-23
Cast: 2024-06-21
Filter: 2024-06-21
Inner Join: 2024-06-21
Lags: 2024-06-21
Left Join: 2024-06-21
Melt: 2024-06-21
Union: 2024-06-21


## Background
This repo contains files for a data frames benchmark. Currently, the data frame pacakges tested include R data.table, Python Polars, R DuckDB, Python Pandas, and R Collapse.

All of the packages are installed as recommended. I'm using Windows 10 OS. If anyone wants to run these on MAC or Linux, please share your results and I will display them. Lastly, I'm running this locally, not on cloud.

The datasets utilized replicates a real world example of a beverage company's data, for 1M, 10M, 100M, and 1B records. The datasets include a date variable, four group variables, and four numeric variables. The benchmark tests each dataset, using the Date variables, then adds additional group variables, and then repeats that with additional numeric variables, for each of the datasets.

<br>

## Current Frameworks Tested
* R data.table: v1.17.99
* R Collapse: v.2.1.5
* R DuckDB: v1.3.2
* Python Polars: v1.3.2
* Python Pandas: v2.3.1

<br>

## Current Operations
* Aggregation (sum)
* Melt
* Cast
* Windowing (lags)
* Union
* Left Join
* Inner Join
* Filter

<br>

## Dataset Attributes
Common attributes across datasets:
* Brand: 13 levels
* Category: 6 levels
* Beverage Flavor: 21 levels
* Four numeric variables
* One Date Variable

<br> 

1M Rows Data
* Customer: 99 levels

10M Rows Data
* Customer: 1071 levels

100M Rows Data
* Customer: 10793 levels

1Bn Rows Data
* Customer: 108017 levels

<br>

## Machine Specs
* Windows 10 OS
* Memory: 256GB
* CPU: 32 cores / 64 threads
* AMD Ryzen CPU

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

### Windowing (lags)
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

### Union
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run Union_datatable.R
* Run Union_DuckDB.R
* Run Union_Polars.py
* Run Union_Pandas.py
* Run CombineResults_Union
* Done!

</details>

### Left Join
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run LeftJoin_datatable.R
* Run LeftJoin_collapse.R
* Run LeftJoin_DuckDB.R
* Run LeftJoin_Polars.py
* Run LeftJoin_Pandas.py
* Run CombineResults_LeftJoin
* Done!

</details>

### Inner Join
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run InnerJoin_datatable.R
* Run InnerJoin_collapse.R
* Run InnerJoin_DuckDB.R
* Run InnerJoin_Polars.py
* Run InnerJoin_Pandas.py
* Run CombineResults_InnerJoin
* Done!

</details>

### Filter
<details><summary> Click here to see steps </summary>

* Fork the repo and clone it to your local machine
* Modify the Path variable at the top of each script to reflect your file location
* Run FakeBevDataBuilds.R to generate the benchmarking datasets
* Run Filter_datatable.R
* Run Filter_collapse.R
* Run Filter_DuckDB.R
* Run Filter_Polars.py
* Run Filter_Pandas.py
* Run CombineResults_Filter
* Done!

</details>

<br>

## Benmark Results

In the plots below the x-axis "Experiments" shows four letters with numbers in front of them. This is what they mean:
* M: millions of rows
* N: number of numeric variables
* D: number of date variables
* G: number of additional group variables

<br>

### Sum Aggregation
#### Total Run Time

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/AggSum_TotalRunTime.PNG)
<details><summary> Click here to see detailed results </summary>

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

### Melt
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Melt_TotalRunTime.PNG)

<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Melt.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Melt.PNG)

<br>

##### With DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Melt_WithDuckDB.PNG)

<br>

##### Without DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Melt_WithoutDuckDB.PNG)

</details>

<br>

### Cast
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Cast_TotalRunTime.PNG)

<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Cast.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Cast.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Cast.PNG)

</details>

<br>

### Windowing (lags)
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Lags_TotalRunTime.PNG)

<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Lags.PNG)

<br>

With DuckDB
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Lags.PNG)

<br>

Without DuckDB
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_WithoutDuckDB_Lags.PNG)

<br>

##### With DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Lags_WithDuckDB.PNG)

<br>

##### Without DuckDB

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Lags_WithoutDuckDB.PNG)

</details>

<br>


### Union
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Union_TotalRunTime.PNG)

<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Union.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Union.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Union_WithDuckDB.PNG)


</details>

<br>

### Left Join
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/LeftJoin_TotalRunTime.PNG)
<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_LeftJoin.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_LeftJoin.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_LeftJoin.PNG)

</details>

<br>

### Inner Join
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/InnerJoin_TotalRunTime.PNG)
<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_InnerJoin.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_InnerJoin.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_InnerJoin.PNG)

</details>

<br>

### Filter
#### Total Run Time
![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/Filter_TotalRunTime.PNG)
<details><summary> Click here to see detailed results </summary>

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults_Filter.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults_Filter.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults_Filter.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1BResults_Filter.PNG)

</details>
