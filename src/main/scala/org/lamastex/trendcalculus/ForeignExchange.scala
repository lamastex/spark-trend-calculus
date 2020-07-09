package org.lamastex.trendcalculus

import java.sql.Timestamp
import java.text.SimpleDateFormat

object ForeignExchange {

    case class FXData(
        time: Option[Timestamp] = None,
        open: Option[Double] = None,
        high: Option[Double] = None,
        low: Option[Double] = None,
        close: Option[Double] = None,
        volume: Option[Int] = None
    )

    def parseFX(str: String): FXData = {
        val tokens = str.split(";")

        val time: Option[Timestamp] = try {
            Some(
                new Timestamp(
                    new SimpleDateFormat("yyyyMMdd HHmmSS")
                        .parse(
                            tokens(0)
                            // .replaceAll("\\s", "")
                        )
                        .getTime
                )
            )
        } finally {
            None
        }

        FXData(
            time = time,
            open = try {Some(tokens(1).toDouble)} finally { None },
            high = try {Some(tokens(2).toDouble)} finally { None },
            low = try {Some(tokens(3).toDouble)} finally { None },
            close = try {Some(tokens(4).toDouble)} finally { None },
            volume = try {Some(tokens(5).toInt)} finally { None }
        )
    }
}