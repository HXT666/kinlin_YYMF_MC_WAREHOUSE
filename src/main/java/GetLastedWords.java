import com.aliyun.odps.io.ArrayWritable;
import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;
import sun.dc.pr.PRError;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Resolve("*->array<String>")
public class GetLastedWords extends Aggregator
{


    private ArrayWritable res = new ArrayWritable(Text.class);

    public GetLastedWords() {
        super();
    }

    @Override
    public void setup(ExecutionContext ctx) throws UDFException {
        super.setup(ctx);
    }

    @Override
    public void close() throws UDFException {
        super.close();
    }

    /**
     * 创建聚合Buffer
     * @return Writable 聚合buffer
     */
    @Override
    public Writable newBuffer() {
        return new Text();
    }

    /**
     *
     * @param buffer 聚合buffer,区内合并
     * @param args sql中调用UDAF时指定的参数，不能为null，但是args里面的元素可以为null，代表输入的数据是null
     * @throws UDFException
     */
    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {
        //区内合并
        Text buf = (Text) buffer;

        if (args.length!=0){
            for (Writable arg:args) {
                if (!Objects.isNull(arg)&& !"".equals(arg.toString())){
                    String word  = arg.toString()+",";
                    buf.append(word.getBytes(StandardCharsets.UTF_8),0,word.getBytes().length);
                }else {
                    String  word = " ,";
                    buf.append(word.getBytes(StandardCharsets.UTF_8),0,word.getBytes().length);
                }
            }
        }
    }

    /**
     *
     * @param buffer 聚合buffer、区间合并
     * @param partial 分片聚合结果
     * @throws UDFException
     */
    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {
        //区间合并
        Text buf = (Text)buffer;
        Text par = (Text)partial;
        if (!Objects.isNull(partial)&& !"".equals(par.toString())) {
            buf.append(par.getBytes(),0,par.getBytes().length-1);
        }
    }

    /**
     * 生成最终结果
     * @param buffer
     * @return Object UDAF的最终结果
     * @throws UDFException
     */
    @Override
    public Writable terminate(Writable buffer) throws UDFException {

        //生成最终结果
        Text buf =  (Text) buffer;
        String[] split = buf.toString().split(",");
        System.out.println(split.toString());
        if (split.length!=0){
            Writable[] writables = new Writable[split.length];
            for (int i = 0; i < split.length; i++) {
                writables[i]= new Text(split[i]);
            }
            res.set(writables);
            return res;
        }else {
            return null;
        }

    }

//    public static void main(String[] args) {
////        Text t = new Text();
////        t.set("a,");
////        System.out.println(t);
////        t.set("b,");
////        System.out.println(t);
////        t.append("c,".getBytes(),0,"c,".getBytes().length-1);
////        System.out.println(t);
//
//        ArrayWritable arrayWritable = new ArrayWritable(Text.class);
////        Writable[] writables = arrayWritable.get();
//        Writable[] writables1 =  new Writable[10];
//        writables1[0] = new Text("a");
//        arrayWritable.set(writables1);
//        System.out.println(arrayWritable.toString());
//
//    }
}
