package com.songshu.snt.sharding.sphere.algorithm;

import com.google.common.collect.Range;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashSet;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingAlgorithm;
import org.apache.shardingsphere.api.sharding.standard.RangeShardingValue;

/**
 * Between的分表算法实现
 *
 * @author 鼠笑天
 */
public class TableRangeShardingAlgorithm implements RangeShardingAlgorithm<Long> {

    @Override
    public Collection<String> doSharding(Collection<String> collection, RangeShardingValue<Long> rangeShardingValue) {
        Collection<String> collect = new HashSet<String>();
        Range<Long> valueRange = rangeShardingValue.getValueRange();
        for (Long i = valueRange.lowerEndpoint(); i <= valueRange.upperEndpoint(); i++) {
            for (String target : collection) {
                if (target.endsWith(new DecimalFormat("000").format(i % collection.size() + 1))) {
                    collect.add(target);
                }
            }
        }
        return collect;
    }
}
