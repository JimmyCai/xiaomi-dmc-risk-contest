package com.xiaomi.ad.others

/**
  * Created by cailiming on 17-9-28.
  */
case class Label(user: String, timeLabel: String, label: Int)
case class UserActions(user: String, timeAction: String, actions: String)
case class UserActionLabel(user: String, timeLabel: String, timeAction: String, actions: String, label: Int)

case class UALProcessed(user: String, time: String, actions: scala.collection.Map[String, scala.collection.Map[Int, Double]], label: Int)
