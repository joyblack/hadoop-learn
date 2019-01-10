package com.zhaoyi.table.combine;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class BookReducer extends Reducer<Text, BookBean, BookBean, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<BookBean> values, Context context) throws IOException, InterruptedException {
        List<BookBean> orderBeans = new ArrayList();
        String bookName = "书籍名称";
        for (BookBean value : values) {
            if(BeanSource.PREFIX_BOOK_FILE.equals(value.getBeanSource())){
                bookName = value.getBookName();
            }else{// order table
                // 保存每一条订单信息
                BookBean temp = new BookBean();
                try {
                    // copy bean.
                    BeanUtils.copyProperties(temp, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(temp);
            }
        }
        // 拼接双标并写入数据
        for (BookBean data : orderBeans) {
            data.setBookName(bookName);
            context.write(data,NullWritable.get());
        }
    }
}
