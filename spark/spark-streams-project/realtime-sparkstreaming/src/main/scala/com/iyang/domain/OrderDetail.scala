package com.iyang.domain

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
case class OrderDetail(
                        id: String,
                        order_id: String,
                        sku_name: String,
                        sku_id: String,
                        order_price: String,
                        img_url: String,
                        sku_num: String
                      ) {}
