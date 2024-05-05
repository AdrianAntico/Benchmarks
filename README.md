This repo contains files for a data frames benchmark. Currently, the data frame pacakges tested include R's data.table, Python's Polars, and DuckDB. Others to come.

I plan on adding additional operations to this benchmark over time. Pull requests are welcome and so are issues and ideas!

## Replicate Benchmarks
1. Fork the repo
2. Modify the Path variable at the top of each file to reflect your file location
3. Run FakeBevDataBuilds.R to generate the benchmarking datasets
4. Run BenchmarkingDatatable.R
5. Run BenchmarkingDuckDB.R
6. Run BenchmarkingPolars.py
7. Run BenchmarkCombineResults
8. Done!

<br>

## Benmark Results

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults.PNG)


