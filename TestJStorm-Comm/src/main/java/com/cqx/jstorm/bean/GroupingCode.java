package com.cqx.jstorm.bean;

/**
 * GroupingCode
 *
 * @author chenqixu
 */
public enum GroupingCode {
    /**
     * 类似SQL中的group by， 保证相同的Key的数据会发送到相同的task，
     * 原理是 对某个或几个字段做hash，然后用hash结果求模得出目标taskId
     */
    FIELDSGROUPING("fieldsGrouping"),
    /**
     * target component第一个task
     */
    GLOBALGROUPING("globalGrouping"),
    /**
     * 轮询方式，平均分配tuple到下一级component上
     */
    SHUFFLEGROUPING("shuffleGrouping"),
    /**
     * 本worker优先，如果本worker内有目标component的task，
     * 则随机从本worker内部的目标component的task中进行选择，否则就和普通的shuffleGrouping一样
     */
    LOCALORSHUFFLEGROUPING("localOrShuffleGrouping"),
    /**
     * 本worker优先级最高，如果本worker内有目标component的task，
     * 则随机从本worker内部的目标component的task中进行选择，
     * 本节点优先级其次， 当本worker不能满足条件时，
     * 如果本supervisor下其他worker有目标component的task，则随机从中选择一个task进行发送
     * 当上叙2种情况都不能满足时， 则从其他supervisor节点的目标task中随机选择一个task进行发送。
     */
    LOCALFIRSTGROUPING("localFirstGrouping"),
    /**
     * 随机发送tuple到目标component上，但无法保证平均
     */
    NONEGROUPING("noneGrouping"),
    /**
     * 发送给target component所有task
     */
    ALLGROUPING("allGrouping"),
    /**
     * 发送指定目标task
     */
    DIRECTGROUPING("directGrouping"),
    /**
     * 使用用户接口CustomStreamGrouping选择出目标task
     */
    CUSTOMGROUPING("customGrouping");

    private String code;

    GroupingCode(String code) {
        this.code = code;
    }

    public String getCode() {
        return this.code;
    }
}
