package com.huangyueran.spark.hrx.datacenter;

import org.apache.commons.lang3.StringUtils;

public class clearfunction {

    /*
    检测值是否在最大与最小的区间内
     */
    public boolean check_max_min(Long thisv,Long min,Long max){
        return thisv>=min && thisv<=max;
    }


    /*
       判断非空 为空返回true
     */
    public boolean check_empty(Object thisv){
        boolean resultv=false;

        resultv=thisv==null;

        //字段串追加判断是否为空字符串
        if(!resultv && thisv.getClass()==String.class){
            resultv= StringUtils.isEmpty(String.valueOf(thisv));
        }

        return resultv;
    }

    //public void
}
