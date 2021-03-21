# Spark-Trend-Calculus

How to cite this work:

- Antoine Amend, Johannes Graner and Razeesh Sainudiin (2020-2021). A Scalable Library for Trader-Perceived Financial Events in an Inter-valued Time Series for a Trend-Calculus. https://github.com/lamastex/spark-trend-calculus/

Many thanks to Andrew Morgan and Antoine Amend. The work in 2020 was partly supported by Combient Mix AB through Data Engineering Science Summer Internships.

To detect trends in time series using Andrew Morgan's trend calculus algorithms in Apache Spark and Scala from Antoine Amend's initial implementation.

Andrew's codes and documentation on how to study trends:

- https://github.com/ByteSumoLtd/TrendCalculus-lua
- https://github.com/bytesumo/TrendCalculus/blob/master/HowToStudyTrends_v1.03.pdf

Antoine's codes used to start this library:

- https://github.com/aamend/texata-r2-2017

Example use cases:

- https://github.com/lamastex/spark-gdelt-examples
- https://github.com/lamastex/spark-trend-calculus-examples

## Usage

See Antoine's GitHub for details on how to use his implementation of Trend Calculus, mainly contained in [`TrendCalculus.scala`](src/main/scala/org/lamastex/spark/trendcalculus/TrendCalculus.scala).

The scalable and streamable implementation in Apache Spark is given in [`TrendCalculus2.scala`](src/main/scala/org/lamastex/spark/trendcalculus/TrendCalculus2.scala).

The basic use case is to transform the input time series to a `Dataset[org.lamastex.spark.trendcalculus.TickerPoint]` where `TickerPoint` is a case class in [`Point.scala`](src/main/scala/org/lamastex/spark/trendcalculus/Point.scala) consisting of a ticker `ticker` (`String`), a timestamp `x` (`java.sql.Timestamp`) and a value `y` (`Double`).

This `Dataset` is then used in the constructor of a `TrendCalculus2` object together with a window size (minimum 2) and a `SparkSession` object. 

The `TrendCalculus2` object has a method `reversals` that can be called to (lazily) compute the reversals using the Trend Calculus algorithm.

Given input as the DataFrame `inputDF` with columns `ticker`, `x`, `y`, the whole call looks like 

```
import org.lamastex.spark.trendcalculus._

val spark: SparkSession = ...
val inputDF: DataFrame = ...
val windowSize: Int = ...

val reversalDS = new TrendCalculus2(inputDF.select("ticker", "x", "y").as[TickerPoint], windowSize, spark).reversals
```

This also works when `inputDF` is a streaming DataFrame using Spark Structured Streaming.

For more detailed examples, see https://github.com/lamastex/spark-trend-calculus-examples.

## Included parsers

Parsers for 1-minute foreign exchange data (https://github.com/philipperemy/FX-1-Minute-Data) and stock market data from yfinance (https://github.com/ranaroussi/yfinance). The data from yfinance requires some processing in python before being accepted by the parser.

### Foreign Exchange parser

The scala function is `parseFX` and has as input a string formatted as 

```
"DateTime Stamp;Bar OPEN Bid Quote;Bar HIGH Bid Quote;Bar LOW Bid Quote;Bar CLOSE Bid Quote;Volume"
```

where DateTime Stamp is formatted as `yyyyMMdd HHmmSS`. Open, High, Low and Close are floating point numbers and Volume is an integer (which seems to always be 0).

To read an FX-1-Minute csv directly to an apache spark Dataset, one can use `spark.read.fx1m(filePath)`.

### Yahoo! Finance parser

The scala function is `parseYF` and has as input a string formatted as

```
"DateTime Stamp,Open,High,Low,Close,Adj Close,Volume"
```

where DateTime Stamp is formatted either as `yyyy-MM-dd` or beginning with `yyyy-MM-dd HH:mm:SS` (i.e. `2020-07-09 18:05:00+02:00` is valid but `UTC+2 2020-07-09 18:05:00` is not). Open, High, Low, Close and Adj Close are floating point numbers and Volume can be either an integer or a floating point number.

To read a yfinance csv directly to an apache spark Dataset, one can use `spark.read.yfin(filePath)`.
