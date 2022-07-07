

import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

import java.util.Arrays;

@Resolve({"*->array<string>"})
public class XuQiTest extends Aggregator {

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {

    }

    /**
     * 创建聚合Buffer
     *
     * @return Writable聚合buffer
     */
    @Override
    public Writable newBuffer() {
        return new ArrayWritable(Text.class);
    }

    // private ArrayWritable result = new ArrayWritable(Text.class);

    /**
     * @param buffer 聚合buffer
     * @param args   SQL中调用UDAF时指定的参数，不能为null，但是args里面的元素可以为null，代表对应的输入数据是null
     * @throws UDFException
     */
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        compute((ArrayWritable) buffer, args);
        System.out.println("After iterator: " + Arrays.toString(((ArrayWritable) buffer).get()));
    }

    /**
     * @param buffer  聚合buffer
     * @param partial 分片聚合结果
     * @throws UDFException
     */
    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        ArrayWritable buf2 = (ArrayWritable) partial;
        Writable[] writables2 = buf2.get();

        System.out.println("Before Merge buffer1: " + Arrays.toString(((ArrayWritable) buffer).get()));
        System.out.println("Before Merge buffer2: " + Arrays.toString(writables2));
        compute((ArrayWritable) buffer, writables2);
        System.out.println("After Merge buffer1: " + Arrays.toString(((ArrayWritable) buffer).get()));
        System.out.println("After Merge buffer2: " + Arrays.toString(writables2));

    }

    private static void compute(ArrayWritable buffer, Writable[] writables2) {
        Writable[] writables1 = buffer.get();
        System.out.println("before compute buffer1: " + Arrays.toString(writables1));
        System.out.println("before compute buffer2: " + Arrays.toString(writables2));

        boolean isNew;
        if (writables1 == null) {
            writables1 = new Writable[writables2.length];

            // NPE
            for (int i = 0; i < writables2.length; i++) {
                writables1[i] = new Text();
            }

            buffer.set(writables1);
            isNew = true;
        } else {
            // 是否是最新记录
            isNew = writables2[writables2.length - 1].toString()
                    .compareTo(writables1[writables1.length - 1].toString()) >= 0;
        }
        System.out.println("isNew: " + isNew);

        for (int i = 0; i < writables2.length; i++) {
            Text text = (Text) writables2[i];
            // 最新记录字段值为 null
            if (text == null) {
                text = new Text();
                writables2[i] = text;
            } else if (isNew && !"".equals(writables2[i].toString())) {
                // 最新记录 且 字段值不为 null，update
                ((Text) writables1[i]).set(text.getBytes());
            } else if (!isNew && "".equals(writables1[i].toString())){
                ((Text) writables1[i]).set(text.getBytes());
            }
        }
        System.out.println("after compute buffer1: " + Arrays.toString(writables1));
        System.out.println("after compute buffer2: " + Arrays.toString(writables2));
    }

    public static void main(String[] args) {
        ArrayWritable arrayWritable = new ArrayWritable(Text.class);

        Writable[] writables = new Writable[3];
        writables[0] = new Text("001");
        writables[2] = new Text("3");

        compute(arrayWritable, writables);
    }

    /**
     * 生成最终结果
     *
     * @param buffer
     * @return Object UDAF的最终结果
     * @throws UDFException
     */
    @Override
    public Writable terminate(Writable buffer) throws UDFException {
        /*ArrayWritable arrayWritable = (ArrayWritable) buffer;
        Writable[] writables = arrayWritable.get();
        arrayWritable.set(null);
        return new ArrayWritable(Text.class, writables);*/
        System.out.println("Terminate: " + Arrays.toString(((ArrayWritable) buffer).get()));
        return buffer;
    }

    @Override
    public void close() throws UDFException {

    }

}