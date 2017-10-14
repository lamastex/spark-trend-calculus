package com.aamend.texata.html

case class Article(
                    title: String,
                    description: String,
                    content: String,
                    tags: Set[String]
                  )
