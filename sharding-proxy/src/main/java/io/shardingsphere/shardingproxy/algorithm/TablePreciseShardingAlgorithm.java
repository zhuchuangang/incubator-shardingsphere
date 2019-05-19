package io.shardingsphere.shardingproxy.algorithm;

import io.shardingsphere.api.algorithm.sharding.PreciseShardingValue;
import io.shardingsphere.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import java.text.DecimalFormat;
import java.util.Collection;
/**
 * 分片字段值取hashcode % 分表鼠 获得 分片的表
 *
 * @author 鼠笑天
 */
public class TablePreciseShardingAlgorithm implements PreciseShardingAlgorithm<String> {

    @Override
    public String doSharding(final Collection<String> availableTargetNames,
        final PreciseShardingValue<String> shardingValue) {
        Integer index = Math.abs(shardingValue.getValue().hashCode() % availableTargetNames.size());
        for (String target : availableTargetNames) {
            if (target.endsWith(new DecimalFormat("000").format(index + 1))) {
                return target;
            }
        }
        throw new UnsupportedOperationException();
    }

}