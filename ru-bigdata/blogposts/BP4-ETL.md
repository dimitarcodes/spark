# Assignment 4

In this assignment we went over SQL with Spark, dataframes and getting an overview of our data, as well as cleaning it up (preprocessing). We also went over  As an AI student that has already taken Data Mining and Information Modelling & Databases most of the assignment was pretty trivial to go through (not new concepts).

Here are some notable stuff from the tutorial that might come in handy in the future:

## loading from csv

we can load (and cache, lazy evaluation!) a file with a header using:

```spark
%spark
val bagdata = spark.read.format("csv").option("header", true).load("file:///opt/hadoop/share/data/BAG_ADRES.csv").cache()
```

## inspecting the data

we can use the following function to quickly get an idea of what our data looks like:
* `.printSchema` prints the schema of the dataframe so we can inspect each column (variable)'s name, their type and whether they are nullable
* `.show(x)` shows `x` number of entries from the spark dataframe
* `.describe()` shows the following statistics: 
    * count
    * mean value
    * standard deviation
    * min value
    * max value
* `.filter(query)` filters the data by a given query, for example `.filter($"VAR_NAME".isNull)` will return a variable with entries where variable "VAR_NAME" has value Null.
    * Extending this query with `.show(5)` will also print 5 of those entries. (which happens to be one of the tasks given to us by the assignment) the resulting query is 
    ```spark
    %spark
    addrDF.filter($"X_COORD".isNull).show()
    ```
* `.sql(query)` does essentially the same thing as `.filter(query)`, except `query` in this case is a single string that is a proper SQL query. 
    * e.g. `spark.sql("select * from kunst where bouwjaar is null")`, which can be extended with a `.show(5)` to show 5 of the entries matching the query
    * note that sql is a function of the spark object, which knows the table `kunst` because it was created using `data_variable.createOrReplaceTempView("kunst")` 

## Data science issues

There are some entries which are wrong (incorrectly entered or incorrect information was entered) or missing. This is a fundamental issue with datas science in general and a domain specialist needs to be consulted about fixing non-trivial errors. For example the year is 9999 for some of the artworks, producing a mean year higher than the current (2021) one.

## Types

At first all the types are String after reading them from the CSV file. We have to manually convert them to types we deem appropriate. In this particular case we had variables which were entered with a format that isn't natively parse-able by spark, so we had to use a user-defined function in order to "translate" the strings to a data type that's usable for us. Additionally not only are we parsing it to usable format but also translating it in a different format than originally given (different coordinate systems).

## Joining the datasets

we choose to join the artworks against the addresses instead of vice versa, because there could be multiple artworks on the same location, causing the join fail if done in an inverted manner.

## Queries queries queries

After completing the 3 queries at the end we find out that there are a lot of quarters (neighborhoods?) with no artwork situated in them, and from the artworks with no quarter association there is a decently big period of half a century (1948-1999) where each year has an artwork with missing address information, which means a lot of possibly valuable for us data is missing. For some of these entries it could be fixable, like determining the quarter in which their coordinates lie. For others it's an issue that only a consultation with a domain expert can help.