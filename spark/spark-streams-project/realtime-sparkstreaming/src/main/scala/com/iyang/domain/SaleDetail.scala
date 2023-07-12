package com.iyang.domain

/** **
 * author: BaoYang
 * date: 2023/7/12
 * desc: 
 * * */
case class SaleDetail(
                       var order_detail_id: String = null,
                       var order_id: String = null,
                       var order_status: String = null,
                       var create_time: String = null,
                       var user_id: String = null,
                       var sku_id: String = null,
                       var user_gender: String = null,
                       var user_age: Int = 0,
                       var user_level: String = null,
                       var sku_price: Double = 0D,
                       var sku_name: String = null,
                       var sku_num: Int = 0,
                       var dt: String = null,
                       var province_id: String = null,
                       var province_name: String = null,
                       var region_id: String = null,
                       var iso_code: String = null,
                       var iso_3166_2: String = null
                     ) {


}
