package com.iyang.domain

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
case class CouponAlertInfo(
                            id: String,
                            uids: mutable.Set[String],
                            itemIds: mutable.Set[String],
                            events: ListBuffer[String],
                            ts: Long
                          ) {}
