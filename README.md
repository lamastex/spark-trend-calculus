# Texata 2017 - Round 2

![HEADER](/images/header.png)

## The questions

*You are the technical co-founder of a new start-up called texata.ai. 
This business was founded to leverage the vast source of news information available on 
the internet in order to better predict geo political instability in exporting countries, 
turning these events into actionable insights that can be used as financial instruments 
in the oil and gas markets. Using the GDELT (global database of events language and tone) 
dataset provided, conduct analysis relevant to the three business situations below.*

- [Time Series analysis](#TIMESERIES): *Using the GDELT event dataset, can you train a computer to detect arising conflicts 
in a particular region of the globe at the early stage of a political instability?*
- [Network analysis](#NETWORK): *Using the GDELT global knowledge graph database and the game of alliances that 
exists between different regimes and world leaders, can you identify the main 
influencers in the different oil and gas markets?*
- [Inference](#INFERENCE): *Given your newly acquired domain expertise together with the provided benchmarks 
in crude oil (BRENT and OPEC), can you define the influence of a series of successive 
political events in the oil market?*

<a name="TIMESERIES"></a>
## Time Series analysis

My approach will consist on the following

- Extract all event Ids from GKG that relate to oil or gas (resp. `ENV_OIL` and `ENV_GAS` cameo code)
- Retrieve all events from EVENT related to the above by joining 2 datasets (massive `JOIN` operation)
- Extract the media coverage as a normalized number of articles by country
- Plot both the goldstein scale and number of articles over time grouped by country

### Media coverage

Below example shows the (normalized) media coverage for both France and United Kingdom with regards to oil and gas.

![EVENT](/images/FR_UK_OIL-events.png)

That way, I can quickly eye ball any potential outbreak related to the oil and gas markets. 
Programmatically, I define the coverage as the zscore function of the number of articles per country, and extract 
top 1000 tuples country / dates (1000 most massively covered events). 

The idea is then to enrich the full data with the events that took place on those dates, at these places. 

```
+-----------+-------+------------------+
|       date|country|          coverage|
+-----------+-------+------------------+
|2016-03-03 |     NC|13.662072684496462|
|2016-04-14 |     MJ|13.414666979122172|
|2016-02-25 |     NC|12.405574670546713|
|2014-09-25 |     SU|  9.48863214818537|
|2015-07-30 |     GA|  9.29919393241244|
|2016-01-21 |     SZ| 8.868822058237168|
|2016-07-07 |     FJ| 8.484678646900855|
|2017-08-03 |     VE|  8.31103934678676|
|2017-07-27 |     VE|  8.02805049313106|
|2017-02-23 |     MY| 8.022447901213992|
|2015-03-12 |     MP| 8.000639172377282|
|2016-04-07 |     MJ| 7.972090458950071|
|2016-02-25 |     IV| 7.945543203860656|
|2015-07-09 |     EC|7.7897667107480775|
|2016-05-05 |     CA|  7.56970466851033|
|2015-01-15 |     MC| 7.471498021926732|
|2016-01-14 |     IR|7.2391919471156125|
|2016-07-14 |     CH| 7.145798060874983|
|2016-07-14 |     RP| 7.072568219684949|
|2015-07-23 |     GA| 6.950294549668176|
+-----------+-------+------------------+
```

- I extracted only the top 1,000 (in order to limit the number of articles to fetch in a 4h time competition)
- I can safely fetch all articles from online websites using the URLs provided in data model. 
- The list of URLs I get back is around 30K large. On my 10 nodes cluster, I reckon it should take around 30mn to scrape all of those. 
- I only fetch the first third and build an efficient web scraper that I distribute across my 10 nodes.

### Fetching HTML content

For that purpose, I'm using a version of [Goose](https://github.com/GravityLabs/goose/wiki) that I recompiled for Scala 2.11

```
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+
|country|goldstein|title                                                                                                    |date      |
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+
|BL     |-10.0    |Pope's 'homecoming' tour moves from Ecuador to Bolivia                                                   |2015-07-08|
|IV     |-10.0    |One pirate killed, four arrested after raid on hijacked ship                                             |2016-02-22|
|NO     |10.0     |Copter fuselage retrieved, search still on for missing                                                   |2016-04-30|
|VE     |-10.0    |Several nations see Venezuela vote as a sham--Aleteia                                                    |2017-07-31|
|MY     |-10.0    |North Korean diplomat warned to cooperate in Kim Jong Un’s alleged assassination investigation - National|2017-02-25|
|SZ     |-10.0    |22,000 Islamic State jihadists have been killed by coalition, France claims                              |2016-01-22|
|EC     |-10.0    |Pope's 'homecoming' tour moves from Ecuador to Bolivia                                                   |2015-07-08|
|IR     |-10.0    |The Other News: Ayatollah Ali Khamenei                                                                   |2016-01-11|
|MP     |8.0      |India to fund key Mauritian infrastructure projects                                                      |2015-03-12|
|AQ     |7.0      |Southern California monuments would be spared, six others would be reduced – Daily News                  |2017-09-19|
|GA     |7.0      |Morris grateful for LDM help in fire rescue                                                              |2015-07-31|
|FM     |4.0      |Africa and Asia forge stronger alliances                                                                 |2016-08-30|
|NC     |2.8      |Marquesas Islands, French Polynesia: How to get to the world's most remote islands                       |2016-03-04|
|SU     |1.9      |S. Korea holds send-off ceremony for U.N. mission to South Sudan                                         |2014-09-23|
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+
```

Those are the news events that happened on those dates, at those places, 
and that were identified as breaking news articles with regards to either `ENV_OIL` or `ENV_GAS`. 
I reckon I should de-noise this data by looking at the text content, applying some NLP and topic modeling, 
but the second top most article is of a great value already as clearly, piracy off the Ivory coast should have 
strong negative impact in the oil and gas markets.

![PIRACY](/images/piracy.jpg)

Well, now I have enriched my raw data with articles I know could have serious impact on the markets. 
I will (hopefully) be using this information later when inferring series of events that could affect oil and gas price.

<a name="NETWORK"></a>
## Network Analysis

The idea here is to look at the possible connections in the oil and gas markets, 
turning GKG into social graph that can be analysed further. 
My end goal here is to extract relations, infer communities, and find out the common denominator among communities. 
Together with the list of raw articles I managed to extract earlier, 
I should be able to see the influence a particular event may have in this network graph.

First, I extract all GKG events related to `ENV_OIL` or `ENV_GAS`.

In term of community detection, due to the scale of the problem (see below figures), this must be done in parallel. 
I have two possible alternative

- WCC detection: [http://arxiv.org/pdf/1411.0557.pdf](http://arxiv.org/pdf/1411.0557.pdf)
- Louvain modularity: [https://arxiv.org/pdf/0803.0476.pdf](https://arxiv.org/pdf/0803.0476.pdf)

### Processing graph

My graph contains around 2,000,000 vertices, 78,000,000 edges, with each node having 70 connections in average. 
Although I'm not concerned processing this graph, 
I feel concerned processing this graph in the remaining 1h and 40mn. 
For the sake of the competition, I'll remove all edges with less than 100 articles in common between 2 different vertices (persons). 
This can be achieved by first collecting the degrees of each node and then removing the appropriate edge and nodes


```scala
  val subgraph = graph.subgraph(
    (et: EdgeTriplet[String, Long]) => et.attr > 100,
    (_, vData: String) => true
  )

  val subGraphDeg = subgraph.outerJoinVertices(subgraph.degrees)((vId, vData, vDeg) => {
    (vData, vDeg.getOrElse(0))
  }).subgraph(
    (et: EdgeTriplet[(String, Int), Long]) => et.srcAttr._2 > 0 && et.dstAttr._2 > 0,
    (_, vData: (String, Int)) => vData._2 > 0
  ).mapVertices({ case (vId, (vData, vDeg)) =>
    vData
  })
```

This now reduces my dimensions down to ~18,000 vertices, 330,000 edges and an average of 11 connections per node. 
Also, in addition of the community, I execute a simple PageRank as a direct measure of the "influencer" score. 

#### Extracting communities

Here are few examples of different communities I managed to extract (Download [pdf](/images/graph.pdf) for a more detailed picture)

![GRAPH](/images/graph.png)

Not a surprise, Donald Trump is a big player in our graph, and is close to the center of the most important community (random first 20 displayed below)

```
+-----------------+
|           person|
+-----------------+
|    igor shuvalov|
|       mary barra|
|      gerald ford|
|viktor yanukovych|
|      harold hamm|
|     steve bannon|
|arkady dvorkovich|
|     gary johnson|
|    bernie sander|
|      igor sechin|
|    mick mulvaney|
|    sergei lavrov|
|  katya golubkova|
|     ernest moniz|
|   hilary clinton|
|   david petraeus|
| alexander korzun|
|     neil gorsuch|
|   lincoln chafee|
|   laurent fabius|
+-----------------+
```

Interestingly, we have some Russian / Ukranian politician names in here. 
Also Laurent Fabius as ex minister of foreign affairs in France at that time. 
While the main community in that graph is around Donald Trump, Vladimir Putin, Barack obama, etc., 
the second most important community seems to be about Europe, Middle East and African countries (first 20 records below).

```
+------------------+
|            person|
+------------------+
|     dolly edwards|
|     umaru yaradua|
|       olisa metuh|
|      ibe kachikwu|
|       garba shehu|
|    steven sotloff|
|      lenin moreno|
|    stephen harper|
| goodluck jonathan|
|       nnamdi kanu|
|   paolo gentiloni|
|  muhammadu buhari|
|    pierre trudeau|
|  yanis varoufakis|
|enrique pena nieto|
|     sylvie corbet|
|    michael fallon|
|   patrick hodgins|
|federica mogherini|
|     darren palmer|
+------------------+
```

The first observation is that oil and gas does not seem to be a single market, but multiple. 
I'm not an expert, but I know at least 3 indices for benchmarking crude oil

- WTI: Refers to oil extracted from wells in the U.S. and sent via pipeline to Cushing, Oklahoma
- BRENT: Produced by various entities in the north sea
- OPEC: Produced by member of the OPEC (Algeria, Angola, Ecuador, Gabon, Iran, Irak, Kuwait, Libya, Nigeria, etc..)

![crude_oil_globe](/images/crude_oil_globe.jpg)

This graph could show these markets as well defined communities (US + Russia, Europe + MiddleEast)

The fact that goodluck Jonhattan and muhammadu buhari (former and actual president of Nigeria) are "close" to Angela Merkel, David Cameron and Francois Hollande could confirm this theory.

## DETECTING TRENDS IN OIL AND GAS

Now the last bit to get a successful startup. We've been able to extract major news articles around OIL and GAS, we know the group of people connected together, and we potentially extracted the different markets these players are dealing with, it is time to enter to the heart of the subject and look at crude oil price. I use the provided brent data from QUANDL and try to detect trends.

The technique I am using is TrendCalculus. https://bitbucket.org/bytesumo/trendcalculus-public

The concept is to find all the highs and lows in my timeseries data, finding the highest high and lowest low in each moving window. For that purpose, I use a window of a 30 days, expecting to find all highs and lows occurring between 2014 and 2017, transforming that time series as a series of trends.

![BRENT1](/images/brent.png)

Once the trends are identified, I extract the reversals, i.e. the highest high or lowest low that were observed before the trend flip (from rising to decreasing). I report few dates below

```
+-----+--------------------+-----+
|trend|                   x|    y|
+-----+--------------------+-----+
|  LOW|2015-01-13 00:00:...|45.13|
| HIGH|2015-05-13 00:00:...|66.33|
|  LOW|2015-08-24 00:00:...|41.59|
| HIGH|2015-10-08 00:00:...|52.13|
|  LOW|2016-01-20 00:00:...|26.01|
| HIGH|2016-06-08 00:00:...|50.73|
|  LOW|2016-08-02 00:00:...| 40.0|
| HIGH|2016-08-26 00:00:...|49.66|
|  LOW|2016-09-27 00:00:...|44.95|
| HIGH|2016-10-19 00:00:...|51.85|
|  LOW|2016-11-13 00:00:...|41.61|
+-----+--------------------+-----+
```

Also attached in the code, please refer to picture brent_H_L.png
![BRENT2](/images/brent_H_L.png)

<a name="INFERENCE"></a>
## Inference

The rest is pure theory here, as I'm realistically not able to build a successful startup in the next 20mn, but here is my idea:

- Enrich our initial dataframe with the reversal points detected from the BRENT timeseries
- I have raw content that I scraped for most of breaking news articles in the OIL GAS GDELT data
- Hopefully the dates just work fine, I know what articles may have cause the market to rise or fall
- I know who's connected to who, and to what market
- I know the influence an event may have in a community
- I should be able to build enough label data to train a simple classifier (I'd say Naive Bayes here). I will know what event in what country, impacting what community will have a positive or negative effect on the crude oild market.

By exporting my model, I can apply on the 15mn GDELT data so that I will be able to detect rise and fall in near real time.

Thank you!

Antoine







