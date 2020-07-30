package org.lamastex.spark.trendcalculus

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class TsToTrend(windowSize: Int, initLine: Option[Row] = None) extends UserDefinedAggregateFunction {

  val FIRST = 0
  val HIGH = 1
  val LOW = 2
  val SECOND = 3

  val X = 0
  val Y = 1

  val pointSchema = StructType(
    StructField("x", LongType, false) ::
    StructField("y", DoubleType, false) ::
    Nil
  )

  val fhlsSchema = StructType(
    StructField("first", pointSchema, false) ::
    StructField("high", pointSchema, false) ::
    StructField("low", pointSchema, false) ::
    StructField("second", pointSchema, false) ::
    StructField("sign", IntegerType, false) ::
    Nil
  )

  val emptyPoint = Row(0L, 0.0)

  val emptyFhls = Row(
    emptyPoint,
    emptyPoint,
    emptyPoint,
    emptyPoint,
    0
  )

  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType = StructType(
    StructField("point", pointSchema) ::
    Nil
  )

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
    StructField("bufferedPoints", ArrayType(pointSchema)) ::
    StructField("counter", IntegerType) ::
    StructField("result", dataType) ::
    Nil
  )

  // This is the output type of your aggregatation function.
  override def dataType: DataType = ArrayType(
    StructType(
      StructField("fhls", fhlsSchema) ::
      StructField("trend", IntegerType) ::
      StructField("lastFhls", fhlsSchema) ::
      StructField("lastTrend", IntegerType) ::
      StructField("reversal", IntegerType) ::
      Nil
    )
  )

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    val init = initLine.getOrElse(Row(Point(0L, 0.0))).getAs[Point](0)
    val initPoint = Row(init.x, init.y)
    val lastFhls = Row(initPoint, initPoint, initPoint, initPoint, 0)
    buffer(0) = (1 to 2*windowSize).map(_ => Row(0L, 0.0)).toSeq
    buffer(1) = 0
    buffer(2) = Seq[Row](Row(lastFhls,1,emptyFhls,0,0))
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val newPoint = input.getStruct(0)
    val newSeq = buffer.getSeq(0).+:(newPoint) //Prepend new point to buffer list
    val newCounter = (buffer.getInt(1) + 1) % windowSize
    buffer(0) = newSeq.take(2*windowSize)
    buffer(1) = newCounter
    if (newCounter == 0) {
      val prevRes: Row = buffer.getSeq(2).last

      val currFHLS = makeFHLS(newSeq.take(windowSize))
      val prevFHLS = prevRes.getStruct(0)
      val lastTrend = prevRes.getInt(1)
      buffer(2) = getTrend(prevFHLS,currFHLS,lastTrend)
    }
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
    //buffer1(1) = buffer1.getAs[Double](1) + buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    val counter = buffer.getInt(1)

    if (counter == 0) {
      buffer.getSeq(2)
    } else {
      Seq.empty[Row]
    }
    
  }

  private def makeFHLS(points: Seq[Row]): Row = {
    val sortedByVal = points.groupBy(_.getDouble(Y)).map {case (v,vSeq) => (v,vSeq.sortBy(_.getLong(X)))}.toSeq.sortBy(_._1)
    val high = sortedByVal.last._2.head //Should be last head
    val low = sortedByVal.head._2.head //Should be head last
    val List(first,second) = if (low.getLong(X) < high.getLong(X)) List(low,high) else List(high,low)
    val sign = if (first.getDouble(Y) < second.getDouble(Y)) 1 else -1

    Row(first, high, low, second, sign)
  }

  private def getTrend(prevFHLS: Row, currFHLS: Row, lastTrend: Int): Seq[Row] = {
    val prevHigh = prevFHLS.getStruct(HIGH)
    val currHigh = currFHLS.getStruct(HIGH)
    val prevLow = prevFHLS.getStruct(LOW)
    val currLow = currFHLS.getStruct(LOW)

    var currTrend = (
      (currHigh.getDouble(Y) - prevHigh.getDouble(Y)).signum + 
      (currLow.getDouble(Y) - prevLow.getDouble(Y)).signum).signum

    var currRev = (currTrend - lastTrend).signum

    if (currTrend != 0) {
      return Seq[Row](Row(currFHLS,currTrend,prevFHLS,lastTrend,currRev))
    }

    val intrFirst = prevFHLS.getStruct(SECOND)
    val intrSecond = currFHLS.getStruct(FIRST)
    val List(intrLow,intrHigh) = if (intrFirst.getDouble(Y) < intrSecond.getDouble(Y)) {
      List(intrFirst, intrSecond)
    } else {
      List(intrSecond, intrFirst)
    }
    val intrSign = if (intrFirst.getDouble(Y) < intrSecond.getDouble(Y)) {
      1
    } else {
      -1
    }

    var intrTrend = (
      (intrHigh.getDouble(Y) - prevHigh.getDouble(Y)).signum +
      (intrLow.getDouble(Y) - prevLow.getDouble(Y)).signum).signum
    
    currTrend = (
      (currHigh.getDouble(Y) - intrHigh.getDouble(Y)).signum +
      (currLow.getDouble(Y) - intrLow.getDouble(Y)).signum).signum

    if (intrTrend == 0) intrTrend = lastTrend
    if (currTrend == 0) currTrend = intrTrend

    val intrFHLS = Row(
      intrFirst,
      intrHigh,
      intrLow,
      intrSecond,
      intrSign
    )

    val intrRev = (intrTrend - lastTrend).signum
    currRev = (currTrend - intrTrend).signum

    Seq[Row](Row(intrFHLS, intrTrend, prevFHLS, lastTrend, intrRev), Row(currFHLS, currTrend, intrFHLS, intrTrend, currRev))
  }
}