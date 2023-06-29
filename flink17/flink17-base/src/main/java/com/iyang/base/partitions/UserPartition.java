package com.iyang.base.partitions;

import org.apache.flink.api.common.functions.Partitioner;

/****
 * author: BaoYang
 * date: 2023/6/25
 * desc:
 ***/
public class UserPartition implements Partitioner<String> {
    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}
