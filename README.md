This repo contains files for a data frames benchmark. Currently, the data frame pacakges tested include R's data.table, Python's Polars, and DuckDB. Others to come.

I know there is a great benchmark provided by DuckDB, who took over the one that H2O used to host. However, the results they show differ from mine. I utilize different data. Mine includes a Date column used in aggregation, which is showing to be a hassle for Polars. Further, DuckDB is shown to be the fastest on the DuckDB benchmark but that's not what I find. It performs best when only Date is used as a by-variable, but when combined with additional by-variables, DuckDB performs the worst.

I plan on adding additional operations to this benchmark over time. Pull requests are welcome and so are issues and ideas!

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/1MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/10MResults.PNG)

<br>

![](https://github.com/AdrianAntico/Benchmarks/raw/main/Images/100MResults.PNG)


