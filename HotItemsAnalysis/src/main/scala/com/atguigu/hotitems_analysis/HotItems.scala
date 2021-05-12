package com.atguigu.hotitems_analysis

import java.sql.Timestamp
import java.util.Properties
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 需求：每隔 5 分钟输出最近一小时内点击量最多的前 N 个商品。
  */

// 定义输入数据样例类
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //todo  如果是真实场景中，前后差5分钟，肯定不会出现乱序，但是在测试中，读的是文件，可能会出现乱序，为了显示的更合理，设置并行度为1
    // 其实生产环境并行度一般不为1，使用watermark来保证触发window的执行
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 定义事件时间语义

    // 从文件中读取数据，并转换成样例类，提取时间戳生成watermark
//    val inputStream: DataStream[String] = env.readTextFile("D:\\JavaRelation\\Workpaces\\myproject\\bigData\\flink2020\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    // 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadoop102:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")

    val inputStream = env.addSource( new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties) )

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      //提取时间戳生成watermark
      // TODO: 因为是已经排好序的升序的时间戳数据，所以用ascending即可 ，如果是乱序数据要用assignTimestampsAndWatermarks并传入BoundedOutOfOrdernessTimestampExtractor
      .assignAscendingTimestamps(_.timestamp * 1000L)
      //如果是乱序数据就要用下面的方法
//      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(3)) {//最大乱序时间
//        override def extractTimestamp(element: UserBehavior): Long = element.timestamp * 1000L
//      })

    val aggStream: DataStream[ItemViewCount] = dataStream
      .filter(_.behavior == "pv") // 过滤出pv行为数据
      .keyBy("itemId") // 按照商品ID分组   todo 注意，这里返回的是[ItemViewCount，Tuple]!!!  key在后面
      // TODO: 为什么不能用下面这种方式呢
      //  其实可以用，而且更好，用这种方式返回的是KeyedStream[UserBehavior, Long]
      //  key为long型而不是Tuple，更直观更好处理！改过之后，后面的processFunction函数key的类型也要做相应的修改
      //.keyBy(_.itemId)
      // 得到窗口聚合结果
      .timeWindow(Time.hours(1), Time.minutes(5)) // 设置滑动窗口进行统计
      // TODO: 为什么用这个方法来聚合
      //  因为我们在做普通聚合时拿不到窗口的信息，并且输入输出元素类型必须一样 ，用这个方法可以获取到window信息，并可以将聚和结果进行包装处理（类型变化）！
      // TODO: 如果当前数据刚好达到一个窗口截止时间，会触发聚合aggregate操作，但不会触发下面的processFunction排序操作！！
      //  因为processFunction里面的定时器触发时间=windowEnd+1ms
      // TODO: 这里的聚合方法，一个窗口只会触发一次，因为是升序的数据，没有延迟的数据到来 ，
      //  后面的process中的listState的一个key的聚合数据也只有一条，所以每条聚合数据来的时候直接add即可，所以不需要MapState
      .aggregate(new CountAgg(), new ItemViewWindowResult())//第一个参数是预聚合（定义聚合规则），第二个参数定义输出结构数据（入参为预聚合的结果）

    //分组、排序(用到listState、定时器，所以需要大招 process Function)
    // TODO: 为什么这里要重新分组
    //  因为aggregate后的还是一个完整的流数据，无法确定当前数据属于哪个窗口，我们不能根据流中的所有数据排序！！这一点跟sparkstreaming不一样！
    val resultStream: DataStream[String] = aggStream
      .keyBy("windowEnd")    // 按照窗口分组，收集当前窗口内的商品count数据
      .process( new TopNHotItems(5) )     // 自定义处理流程

//    dataStream.print("data")
//    aggStream.print("agg")
    resultStream.print()

    env.execute("hot items")
  }
}

// 自定义预聚合函数AggregateFunction，聚合状态就是当前商品的count值
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  // 每来一条数据调用一次add，count值加一
  override def add(value: UserBehavior, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 示例：求取平均数的AggregateFunction
class AvgTs extends AggregateFunction[UserBehavior, (Long, Int), Long]{
  override def add(value: UserBehavior, accumulator: (Long, Int)): (Long, Int) =
    (accumulator._1 + value.timestamp, accumulator._2 + 1)

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def getResult(accumulator: (Long, Int)): Long = accumulator._1 / accumulator._2

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) =
    (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数WindowFunction   输入是聚合函数的输出，输出可以自定义
class ItemViewWindowResult() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {//注意这里的tuple是flink中的tuple！
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.asInstanceOf[Tuple1[Long]].f0 // TODO: Tuple1的f0就是里面的元素
    val windowEnd: Long = window.getEnd//该window的截止时间
    val count = input.iterator.next()
    // TODO: 输出，本身apply方法没有返回值，用Collector输出
    out.collect(ItemViewCount(itemId, windowEnd, count))
  }
}

// 自定义KeyedProcessFunction
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{
  // 先定义状态：ListState  每次来数据都会往里面塞数据，定时器触发时会清空ListState
  private var itemViewCountListState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemViewCountListState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("itemViewCount-list", classOf[ItemViewCount]))
  }

  //ctx可以通过timerService获取到当前的watermark、定时器、当前处理时间 等等很多东西
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    // 每来一条数据，直接加入ListState即可，因为每条数据都是聚合后的数据，一个窗口只有一个（升序数据，窗口触发就是完整的数据，没有延迟到来的数据）
    itemViewCountListState.add(value)
    // 注册一个windowEnd + 1之后触发的定时器(因为这个窗口中每条数据时间都是windowEnd，必须要等窗口所有数据都到齐，也就是waterMark>windowEnd时)
    // TODO: 这里虽然每条数据都会触发这个方法，重复定义了定时器，
    //  但是没关系，因为定时器是根据时间戳来的，根据windowEnd分组后，一个组中的数据的时间戳（windowEnd）是一样的
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 当定时器触发，可以认为所有窗口统计结果都已到齐，可以排序输出了
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 为了方便排序，另外定义一个ListBuffer，保存ListState里面的所有数据
    //因为itemViewCountListState.get()获取到的是一个iter迭代类型，无法转化成list，不好排序
    val allItemViewCounts: ListBuffer[ItemViewCount] = ListBuffer()
    val iter = itemViewCountListState.get().iterator()
    while(iter.hasNext){
      allItemViewCounts += iter.next()
    }

    // 清空状态
    itemViewCountListState.clear()

    // 按照count大小排序，取前n个
    val sortedItemViewCounts = allItemViewCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
   // val sortedItemViewCounts: ListBuffer[ItemViewCount] = allItemViewCounts.sortWith(_.count > _.count).take(topSize)

    // 将排名信息格式化成String，便于打印输出可视化展示
    val result: StringBuilder = new StringBuilder
    result.append("窗口结束时间：").append( new Timestamp(timestamp - 1) ).append("\n")

    // 遍历结果列表中的每个ItemViewCount，输出到一行
    for( i <- sortedItemViewCounts.indices ){ // TODO: 因为util和to老是分不清，不如直接获取索引来遍历！！起始索引为0
      val currentItemViewCount = sortedItemViewCounts(i)
      result.append("NO").append(i + 1).append(": \t")
        .append("商品ID = ").append(currentItemViewCount.itemId).append("\t")
        .append("热门度 = ").append(currentItemViewCount.count).append("\n")
    }

    result.append("\n==================================\n\n")

    Thread.sleep(1000) // 因为从文件读取数据太快，所以减慢速度，模拟下实时的效果
    out.collect(result.toString())
  }
}