package com.aamend.texata.html

import com.gravity.goose.{Configuration, Goose}
import org.apache.spark.rdd.RDD

class HtmlFetcher(connectionTimeOut: Int = 10000, socketTimeOut: Int = 10000) extends Serializable {

  final val USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.71 Safari/537.36"

  def fetchUrls(urls: RDD[String]): RDD[Article] = {
    urls distinct() mapPartitions fetcher
  }

  private def fetcher(iterator: Iterator[String]) = {
    val conf: Configuration = new Configuration
    conf.setBrowserUserAgent(USER_AGENT)
    conf.setEnableImageFetching(false)
    conf.setConnectionTimeout(connectionTimeOut)
    conf.setSocketTimeout(socketTimeOut)
    val goose = new Goose(conf)
    for (url <- iterator) yield {
      try {
        val article = goose.extractContent(url)
        Article(
          article.title,
          article.metaDescription,
          article.cleanedArticleText,
          article.tags.toSet
        )
      } catch {
        case _: Throwable => null
      }
    }
  }
}
