# Texata 2017 - Round 2

![HEADER](/images/header.png)

## EXTRACTING EVENTS

My first EDA approach will consist on the following

Extract all event Ids from GKG that relate to OIL or GAS (resp. ENV_OIL and ENV_GAS cameo code)
Retrieve all events from EVENT related to the above by joining 2 datasets
Plot both the goldstein scale and number of articles over time per country

![EVENT](/images/FR_UK_OIL-events.png)

That way, I can quickly eye ball a potential outbreak related to oil and gas market. Programmatically, I normalize timeseries of number of articles (ZScore) and extract top 1000 dates with most inportant events. The idea is then to enrich the full dataset with the event that took place those dates. Some interesting results below. As a side note, normalizing the number of events at the country level overcome the constraint of media coverage (certain countries should not be penalized because of Russia, US, UK, etc. having better coverage with regards to oil and gas)

```
+-------------------+-------+------------------+
|               date|country|          articles|
+-------------------+-------+------------------+
|2016-03-03 00:00:00|     NC|13.662072684496462|
|2016-04-14 00:00:00|     MJ|13.414666979122172|
|2016-02-25 00:00:00|     NC|12.405574670546713|
|2014-09-25 00:00:00|     SU|  9.48863214818537|
|2015-07-30 00:00:00|     GA|  9.29919393241244|
|2016-01-21 00:00:00|     SZ| 8.868822058237168|
|2016-07-07 00:00:00|     FJ| 8.484678646900855|
|2017-08-03 00:00:00|     VE|  8.31103934678676|
|2017-07-27 00:00:00|     VE|  8.02805049313106|
|2017-02-23 00:00:00|     MY| 8.022447901213992|
|2015-03-12 00:00:00|     MP| 8.000639172377282|
|2016-04-07 00:00:00|     MJ| 7.972090458950071|
|2016-02-25 00:00:00|     IV| 7.945543203860656|
|2015-07-09 00:00:00|     EC|7.7897667107480775|
|2016-05-05 00:00:00|     CA|  7.56970466851033|
|2015-01-15 00:00:00|     MC| 7.471498021926732|
|2016-01-14 00:00:00|     IR|7.2391919471156125|
|2016-07-14 00:00:00|     CH| 7.145798060874983|
|2016-07-14 00:00:00|     RP| 7.072568219684949|
|2015-07-23 00:00:00|     GA| 6.950294549668176|
+-------------------+-------+------------------+
```

Extracting the top 1000, I can safely pull all articles from online websites on those days, for these countries, around OIL and GAS. I simply need to join back to original dataset. The list of URL I can get back is around 20K large. On my 10 nodes cluster, I reckon it should take around 30mn to scrape all those. I retrieve only the first 10'000 and build a webscraper properly distributed across my 10 nodes.

```
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+----+
|country|goldstein|title                                                                                                    |date      |abs |
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+----+
|BL     |-10.0    |Pope's 'homecoming' tour moves from Ecuador to Bolivia                                                   |2015-07-08|10.0|
|IV     |-10.0    |One pirate killed, four arrested after raid on hijacked ship                                             |2016-02-22|10.0|
|NO     |10.0     |Copter fuselage retrieved, search still on for missing                                                   |2016-04-30|10.0|
|VE     |-10.0    |Several nations see Venezuela vote as a sham--Aleteia                                                    |2017-07-31|10.0|
|MY     |-10.0    |North Korean diplomat warned to cooperate in Kim Jong Un’s alleged assassination investigation - National|2017-02-25|10.0|
|SZ     |-10.0    |22,000 Islamic State jihadists have been killed by coalition, France claims                              |2016-01-22|10.0|
|EC     |-10.0    |Pope's 'homecoming' tour moves from Ecuador to Bolivia                                                   |2015-07-08|10.0|
|IR     |-10.0    |The Other News: Ayatollah Ali Khamenei                                                                   |2016-01-11|10.0|
|MP     |8.0      |India to fund key Mauritian infrastructure projects                                                      |2015-03-12|8.0 |
|AQ     |7.0      |Southern California monuments would be spared, six others would be reduced – Daily News                  |2017-09-19|7.0 |
|GA     |7.0      |Morris grateful for LDM help in fire rescue                                                              |2015-07-31|7.0 |
|FM     |4.0      |Africa and Asia forge stronger alliances                                                                 |2016-08-30|4.0 |
|NC     |2.8      |Marquesas Islands, French Polynesia: How to get to the world's most remote islands                       |2016-03-04|2.8 |
|SU     |1.9      |S. Korea holds send-off ceremony for U.N. mission to South Sudan                                         |2014-09-23|1.9 |
+-------+---------+---------------------------------------------------------------------------------------------------------+----------+----+
```

Those are the news events on those dates, for those identified OIL and GAS events. I reckon we should de-noise the data of GKG, but the second top most article above is already of a great value. Basically shows a possible correlation of a piracy in the Somalia coast to the OIL and GAS market.

![PIRACY](/images/piracy.jpg)

Well, now I have enriched GDELT dataset with articles I know may have had serious impact on the OIL and GAS market for some countries. I will (hopefully) be using this information when inferring oil and gas price.

Let's move to the second topic from now, the OIL and GAS influencers.

## NETWORK ANALYSIS

The idea here is to look at possible connections in the oil and gas market, turning GKG into graph that can be analyse further. My end goal here is to find relations, infer communities, and finding out the countries / locations that those key people belong to / act upon. Together with the list of events I extracted from above, I'll be able to see the influence on a particular event in this network graph, finding out possible alliances

First, I extract all GKG events related to OIL and GAS.

In term of community detection, due to the scale of the problem, this must be done in parallel. I have two different approaches I may be using here

- WCC connections [http://arxiv.org/pdf/1411.0557.pdf](http://arxiv.org/pdf/1411.0557.pdf)
- Louvain modularity [https://arxiv.org/pdf/0803.0476.pdf](https://arxiv.org/pdf/0803.0476.pdf)

My graph contains ~2,000,000 vertices, ~78,000,000 edges, with each node having 70 connections in average. Although I'm not concerned processing this graph, I feel concerned processing this graph in the remaining 1h and 40mn. For the sake of the competition, I'll remove all connections with less than 100 articles in common between 2 different persons. This is done by collecting degree of each node and remove the appropriate edge and nodes


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

This now reduces my graph to ~18,000 vertices, 330,000 edges and an average of 11 connections. This filter allows me to move on with the community detection using WCC. The interesting thing around WCC is the approach they use, directly inspired from social networks. Because communities are group of vertices that are tightly coupled, they should share a larger number of triangles among themselves that they share across different groups. 

In addition of the community, I execute a simple PageRank as an "influencer" score. 

Here are few examples of different communities (better resolution [here](/images/graph.pdf))

![GRAPH](/images/graph.png)

#### Communities

Not a surprise, Donald Trump is in our graph, and is center of the most important community (top 20 displayed below)

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

Interestingly, we have some Russian / Ukranian politician names in here. Also Laurent Fabius as ex minister in France. While the main community in that graph is around Donald Trump, Vladimir Putin and Barack obama, the second most important community is about Europe and Middle East countries (first 20 records below).

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

What is interesting is that OIL and GAS is not one market, but multiple. I'm not an expert, but I know 2 indices for benchmarking crude oil

BRENT: Produced by various entities in the north sea
OPEC: Produced by member of the OPEC (Algeria, Angola, Ecuador, Gabon, Iran, Irak, Kuwait, Libya, Nigeria, etc..)
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

## CONNECTING THE DOTS

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







