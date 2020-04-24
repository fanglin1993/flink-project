package com.scz.flink.core

import org.apache.flink.api.scala._

/**
  * Created by shen on 2019/12/22.
  */
object DataSetTransformationApp {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
//    joinFunction(env)
    crossFunction(env)
  }

  def joinFunction(env:ExecutionEnvironment): Unit = {
    val data1 = env.fromCollection(List(
      (1, "PK哥"), (2, "J哥"), (3, "小队长"), (4, "猪头呼")
    ))
    val data2 = env.fromCollection(List(
      (1, "北京"), (2, "上海"), (3, "成都"), (5, "杭州")
    ))
//    data1.join(data2).where(0).equalTo(0).apply((first,second)=>{
//      (first._1, first._2, second._2)
//    }).print()

    data1.fullOuterJoin(data2).where(0).equalTo(0).apply((first,second)=>{
      if(first == null) {
        (second._1, "-", second._2)
      } else if(second == null){
        (first._1, first._2, "-")
      } else {
        (first._1, first._2, second._2)
      }
    }).print()
  }

  def crossFunction(env : ExecutionEnvironment): Unit = {
    val data1 = env.fromCollection(List("曼联","曼城"))
    val data2 = env.fromCollection(List(3, 1, 0))

    data1.cross(data2).print()
  }

}
