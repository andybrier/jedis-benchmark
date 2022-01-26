package jedis;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.List;

/**
 * @author  chunhua.zhang
 */
public class CommandLineArgs
{
    @Parameter
    public List<String> parameters = Lists.newArrayList();

    @Parameter(names = "-n", description = "No. of operations")
    public Integer noOps = 100000;

    @Parameter(names = "-c", description = "No of connections")
    public Integer noConnections = 1;

    @Parameter(names = "-h", description = "Host")
    public String host = "localhost";

    @Parameter(names = "-r", description = "random key space")
    public Integer random =  1000;

    @Parameter(names = "-p", description = "port")
    public Integer port = 6379;

    @Parameter(names = "-a", description = "Password of server")
    public String password = "";

    @Parameter(names = "-d", description = "data size in bytes")
    public Integer dataSize = 100;

    @Parameter(names = "-m", description = "size of mset or mget")
    public Integer mSize = 100;

}
