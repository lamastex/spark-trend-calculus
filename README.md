# Spark-Trend-Calculus

To detect trends in time series using Andrew Morgan's trend calculus algorithms in Apache Spark and Scala from Antoine Amend's initial implementation.

Andrew's codes and documentation on how to study trends:

- https://github.com/ByteSumoLtd/TrendCalculus-lua
- https://github.com/bytesumo/TrendCalculus/blob/master/HowToStudyTrends_v1.03.pdf

Antoine's codes used to start this library:

- https://github.com/aamend/texata-r2-2017

Example use cases:

- https://github.com/lamastex/spark-texata-2020
- 

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