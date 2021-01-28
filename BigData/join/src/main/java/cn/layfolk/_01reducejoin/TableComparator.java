package cn.layfolk._01reducejoin;

import cn.layfolk.bean.TableBean;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Author 王津
 * @Date 2020/8/2
 * @Version 1.0
 */
public class TableComparator extends WritableComparator {

    public TableComparator() {
        super(TableBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TableBean oa = (TableBean) a;
        TableBean ob = (TableBean) b;
        return oa.getP_id().compareTo(ob.getP_id());
    }
}
