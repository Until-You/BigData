package cn.layfolk._01reducejoin;

import cn.layfolk.bean.TableBean;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class TableReducer2 extends Reducer<TableBean, NullWritable, TableBean, NullWritable> {

    @Override
    protected void reduce(TableBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        //这种写法，需要有个分组
        Iterator<NullWritable> iterator = values.iterator();
        iterator.next();
        String pName = key.getPname();
        while (iterator.hasNext()){
            iterator.next();
            key.setPname(pName);
            context.write(key,NullWritable.get());
        }

    }
}
