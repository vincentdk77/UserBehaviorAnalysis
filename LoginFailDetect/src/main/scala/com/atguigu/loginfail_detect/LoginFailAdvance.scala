package com.atguigu.loginfail_detect

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * 改良版
 * 来一条数据，如果是成功的，直接清空状态
 * 如果是失败的，判断前后两次失败的时间差是否小于2s，就立即报警
 * 优点：不需要定时器，非得2s后才报警
 * 缺点：如果需求是2s内3次或者更多次登录失败，判断就非常复杂！
 *        并且如果是乱序数据就可能有bug，比如42,45,43,44   ，只会输出（43,44），不会输出（42，43）
 * 考虑使用CEP（Complex Event Processing，复杂事件处理）
 */
object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val inputStream = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提起时间戳和watermark
    val loginEventStream = inputStream
      .map( data => {
        val arr = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      } )
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // todo 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream = loginEventStream
      .keyBy(_.userId)
      .process( new LoginFailWaringAdvanceResult() )

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWaringAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]{
  // 定义状态，保存当前所有的登录失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if( value.eventType == "fail" ){
      // 1. 如果是失败，进一步做判断
      val iter = loginFailListState.get().iterator()
      // 判断之前是否有登录失败事件
      if(iter.hasNext){
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent = iter.next()
        if( value.timestamp < firstFailEvent.timestamp + 2 ){
          // 如果在2秒之内，输出报警
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2. 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}