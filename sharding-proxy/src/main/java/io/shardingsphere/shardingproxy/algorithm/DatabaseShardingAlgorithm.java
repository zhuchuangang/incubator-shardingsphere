package io.shardingsphere.shardingproxy.algorithm;

import io.shardingsphere.api.algorithm.sharding.PreciseShardingValue;
import io.shardingsphere.api.algorithm.sharding.standard.PreciseShardingAlgorithm;
import io.shardingsphere.core.exception.ShardingConfigurationException;
import io.shardingsphere.core.util.InlineExpressionParser;
import io.shardingsphere.core.yaml.sharding.YamlTableRuleConfiguration;
import io.shardingsphere.shardingproxy.config.yaml.YamlProxyRuleConfiguration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 分片字段值取hashcode % 分片数 获得 分片数据库
 *
 * @author 鼠笑天
 */
public class DatabaseShardingAlgorithm implements PreciseShardingAlgorithm<String> {

    /**
     * 分表对于的表个数
     */
    private static Map<String, Integer> tableCountMap = new HashMap<String, Integer>();
    /**
     * 配置信息
     */
    public static Map<String, YamlProxyRuleConfiguration> CONFIG;

    @Override public String doSharding(final Collection<String> availableTargetNames,
        final PreciseShardingValue<String> shardingValue) {
        Integer tableCount = tableCountMap.get(shardingValue.getLogicTableName());
        if (tableCount == null) {
            YamlTableRuleConfiguration tableRuleConfiguration = null;
            for (YamlProxyRuleConfiguration configuration : CONFIG.values()) {
                tableRuleConfiguration =
                    configuration.getShardingRule().getTables().get(shardingValue.getLogicTableName());
                if (tableRuleConfiguration != null) {
                    break;
                }
            }
            if (tableRuleConfiguration == null) {
                throw new ShardingConfigurationException(shardingValue.getLogicTableName() + "表设置分配配置！");
            }
            List<String> actualDataNodes =
                new InlineExpressionParser(tableRuleConfiguration.getActualDataNodes()).splitAndEvaluate();
            tableCountMap.put(shardingValue.getLogicTableName(), actualDataNodes.size());
            tableCount = actualDataNodes.size();
        }
        Integer databaseCount = availableTargetNames.size();
        Integer tableCountPreDatabase = tableCount / databaseCount;
        Integer index = Math.abs((shardingValue.getValue().hashCode() / tableCountPreDatabase) % databaseCount);
        return (String)availableTargetNames.toArray()[Math.abs(index)];
    }
}