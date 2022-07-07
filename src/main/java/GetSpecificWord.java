import com.aliyun.odps.io.Text;
import com.aliyun.odps.io.Writable;
import com.aliyun.odps.udf.Aggregator;
import com.aliyun.odps.udf.ExecutionContext;
import com.aliyun.odps.udf.UDF;
import com.aliyun.odps.udf.UDFException;
import com.aliyun.odps.udf.annotation.Resolve;

import java.io.IOException;
@Resolve("String,String->String")
public class GetSpecificWord extends Aggregator {


    public GetSpecificWord() {
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

    @Override
    public Writable newBuffer() {
        return new Text();
    }

    @Override
    public void iterate(Writable buffer, Writable[] args) throws UDFException {

    }

    @Override
    public Writable terminate(Writable buffer) throws UDFException {
        return null;
    }

    @Override
    public void merge(Writable buffer, Writable partial) throws UDFException {

    }
}
