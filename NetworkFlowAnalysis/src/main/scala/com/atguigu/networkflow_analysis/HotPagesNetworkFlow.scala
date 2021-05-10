package com.atguigu.networkflow_analysis

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * 实时热门页面流量统计：
 *  读业务服务器日志，每5秒钟输出一次最近10分钟的页面流量统计
 *  用户登录的入口流量、不同页面的访问流量都
 */

// 定义输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, timestamp: Long, method: String, url: String)

// 定义窗口聚合结果样例类
case class PageViewCount(url: String, windowEnd: Long, count: Long)

object HotPagesNetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据，转换成样例类并提取时间戳和watermark
//    val inputStream = env.readTextFile("D:\\Projects\\BigData\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    val inputStream = env.socketTextStream("localhost", 7777)

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(" ")
        // 对事件时间进行转换，得到时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime
        ApacheLogEvent(arr(0), arr(1), ts, arr(5), arr(6))
      } )
      // TODO: 数据乱序时间跨度比较大：大概一分钟，watermark最大延迟时间应该怎么设置呢？
      //  不应该设置太大，否则就会有很多数据等待，实时效果就不好了
      //  我们可以使用.allowedLateness(实际最大延迟时间1分钟) —— 允许处理迟到的数据
      //  当某个数据的时间戳属于的所有窗口都已经关了时，才会输出到侧输出流！！
      //    （比如当前数据时间戳：10:14:51秒，如果他属于的最大的窗口[10:14:50,10:24:50) 已关闭，此时才会输出到侧输出流）
      //  步骤：waterMark设置小一点 + window滑动步长设置大一点 + 使用allowedLateness处理1分钟内的数据，使用侧输出流处理allowedLateness也没有处理到的数据
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
        override def extractTimestamp(element: ApacheLogEvent): Long = element.timestamp
      })

    // 进行开窗聚合，以及排序输出
    val aggStream = dataStream
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(10), Time.seconds(5))
        .allowedLateness(Time.minutes(1))
        .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
      .aggregate(new PageCountAgg(), new PageViewCountWindowResult())

    val resultStream = aggStream
      .keyBy(_.windowEnd)
      .process(new TopNHotPages(3))

    dataStream.print("data")
    aggStream.print("agg")
    aggStream.getSideOutput(new OutputTag[ApacheLogEvent]("late")).print("late")
    resultStream.print()
    env.execute("hot pages job")
  }
}

class PageCountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long]{
  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class PageViewCountWindowResult() extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

class TopNHotPages(n: Int) extends KeyedProcessFunction[Long, PageViewCount, String]{
  // TODO: 这里应该用MapState+两个定时器
  //  每条聚合数据只更新指定key的数据+到达指定时间才清空state
  //  不能用ListState，因为在延迟到来的数据时，无法更新数据
//  lazy val pageViewCountListState: ListState[PageViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[PageViewCount]("pageViewCount-list", classOf[PageViewCount]))
  lazy val pageViewCountMapState: MapState[String, Long] =
          getRuntimeContext.getMapState(new MapStateDescriptor[String, Long]("pageViewCount-map", classOf[String], classOf[Long]))

  override def processElement(value: PageViewCount, ctx: KeyedProcessFunction[Long, PageViewCount, String]#Context,
                              out: Collector[String]): Unit = {
//    pageViewCountListState.add(value)
    pageViewCountMapState.put(value.url, value.count)

    // TODO:这里不可以只定义一个定时器，因为我们前面有  allowedLateness +sideOutputLateData 做了延迟处理，
    //  所以没有及时处理的数据后面过来的时候（定时器已经触发过，又来了老数据，并且watermark没有更新时），这个定时器暂时无法使用！ 但是该定时器会在下一次watermark更新的时候触发！
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    // 另外注册一个定时器，1分钟之后触发，这时窗口已经彻底关闭，不再有聚合结果输出，可以清空状态
    // TODO: 定义了两个定时器，每个定时器达到触发时间，都会触发onTimer方法 ，第二个定时器触发时只需要去清空MapState即可
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    /*val allPageViewCounts: ListBuffer[PageViewCount] = ListBuffer()
    val iter = pageViewCountListState.get().iterator()
    while(iter.hasNext)
      allPageViewCounts += iter.next()
      */

    // 判断定时器触发时间，如果已经是窗口结束时间1分钟之后，那么直接清空状态
    // 第二个定时器触发时，清空状态
    if( timestamp == ctx.getCurrentKey + 60000L ){
      pageViewCountMapState.clear()
      return
    }

    val allPageViewCounts: ListBuffer[(String, Long)] = ListBuffer()
    val iter = pageViewCountMapState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allPageViewCounts += ((entry.getKey, entry.getValue))
    }

//    // 提前清空状态
//    pageViewCountListState.clear()

    // 按照访问量排序并输出top n
    val sortedPageViewCounts = allPageViewCounts.sortWith(_._2 > _._2).take(n)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for( i <- sortedPageViewCounts.indices ){
      val currentItemViewCount = sortedPageViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("页面URL = ").append(currentItemViewCount._1).append("\t")
        .append("热门度 = ").append(currentItemViewCount._2).append("\n")
    }

    result.append("\n==================================\n\n")

    Thread.sleep(1000)
    out.collect(result.toString())
  }
}