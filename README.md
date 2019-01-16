# cube query
## version = 1.0.0

## introduction
> ### This is a simple way to query data cubes on spark.
> ### The only thing you should do is to create a cube.ini file under conf.
> ### Supports parquet, csv, and json.

## query
1. ### Choose your target data by clicking on buttons under "table name". (see feature.1)
2. ### Key in SQL to query tables, but please note that now we only support one table query at a time.

## plot
1. ### Select column for x-axis and y-axis. You may group by x-column then apply aggregate functions on y-columns; Note that you may choose multi y-columns.
2. ### You may experiment on various plot types that we offer.
3. ### By choosing a cohort column, it would first group by cohort column, then generate a graph for each cohort. Remember: you can choose only a set of x-column and y-column for better comparison between cohorts.



