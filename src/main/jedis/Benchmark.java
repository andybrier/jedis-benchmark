package jedis;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

/**
 * @author abhishekk
 */
public class Benchmark
{
    private final int noOps_;
    private final LinkedBlockingQueue<Long> setRunTimes = new LinkedBlockingQueue<Long>();
    private final LinkedBlockingQueue<Long> getRunTimes = new LinkedBlockingQueue<Long>();

    private  int msize;
    private int dataSize;
    private int noThreads;
    private JedisPool pool;

    CountDownLatch setLatch;
    CountDownLatch getLatch ;

    private int r;


    public Benchmark(int r, final int noOps, final int noThreads,  final String host, final int port, int dataSize, String password, int msize)
    {
        this.r = r;
        this.noOps_ = noOps;
        //pool config
        GenericObjectPoolConfig objectPoolConfig = new GenericObjectPoolConfig();
        objectPoolConfig.setMinIdle(10);
        objectPoolConfig.setMaxIdle(50);
        objectPoolConfig.setTestOnBorrow(false);

        if(password != "") {
            pool = new JedisPool(objectPoolConfig, host, port, 1000, password);
        }else{
            pool = new JedisPool(objectPoolConfig, host, port);
        }

        this.msize = msize;
        this.dataSize = dataSize;
        this.noThreads = noThreads;

        setLatch = new CountDownLatch(noOps * noThreads );
        getLatch = new CountDownLatch(noOps * noThreads );

    }

    /**
     * mset
     */
    class MSetThread extends Thread {

        public MSetThread(String name){
            super(name);
        }

        public void run() {
            Jedis jedis = pool.getResource();

            long start = System.currentTimeMillis();

            for(int i = 0; i < noOps_; i++) {

                String msetArgs[] = new String[msize * 2];
                for(int j = 0; j < msize * 2; ){
                    String key = String.valueOf(new Random().nextInt(r));
                    String value = RandomStringUtils.random(dataSize);
                    msetArgs[j] = key;
                    msetArgs[j+1] = value;
                    j = j + 2;
                }

                StopWatch watch = StopWatch.createStarted();
                String result = jedis.mset(msetArgs);
                watch.stop();
                setRunTimes.offer(watch.getTime(TimeUnit.MICROSECONDS));
                setLatch.countDown();
            }

            pool.returnResource(jedis);

            long taken = System.currentTimeMillis() - start;
            long qps =  noOps_ / ( taken / 1000);
            double avg_rt = (double)taken / (double)noOps_;
            System.out.println("Finish MSET, TotalCount: " + noOps_ + ", TotalCost(ms): " + taken + " , QPS(r/s): " + qps + ", AVG RT(ms):" + avg_rt ) ;
        }
    }

    /**
     * mset
     */
    class MGetThread extends  Thread {

        public MGetThread(String name){
            super(name);
        }

        public void run() {

            long start = System.currentTimeMillis();
            Jedis jedis = pool.getResource();

            for(int j = 0; j < noOps_; j++) {

                String mgetArgs[] = new String[msize];
                for (int i = 0; i < msize; i++) {
                    String key = String.valueOf(new Random().nextInt(r));
                    mgetArgs[i] = key;
                }

                StopWatch watch = StopWatch.createStarted();
                List<String> result = jedis.mget(mgetArgs);
                watch.stop();
                getRunTimes.offer(watch.getTime(TimeUnit.MICROSECONDS));
                getLatch.countDown();
            }

            pool.returnResource(jedis);

            long taken = System.currentTimeMillis() - start;
            long qps =  noOps_ / ( taken / 1000);
            double avg_rt = (double)taken / (double)noOps_;
            System.out.println("Finish MGET, TotalCount: " + noOps_ + ", TotalCost(ms): " + taken + " , QPS(r/s): " + qps + ", AVG RT(ms):" + avg_rt ) ;

        }
    }


    public void performBenchmark(String flag) throws InterruptedException
    {

        if(flag.equals("setget") || flag.equals("set")) {
            System.out.println("-------------- mset -----------------");
            StopWatch watch = StopWatch.createStarted();
            for (int i = 0; i < noThreads; i++) {
                new MSetThread("set thread").start();
            }
            setLatch.await();
            watch.stop();
            printStats(watch.getTime(TimeUnit.MILLISECONDS), setRunTimes);
        }

        if(flag.equals("setget") || flag.equals("get")) {
            System.out.println("-------------- mget -----------------");

            StopWatch watch = StopWatch.createStarted();
            for (int i = 0; i < noThreads; i++) {
                new MGetThread("get thread").start();
            }
            getLatch.await();
            watch.stop();
            printStats(watch.getTime(TimeUnit.MILLISECONDS), getRunTimes);
        }
    }


    public void printStats(long taken, LinkedBlockingQueue<Long>  record)
    {
        List<Long> points = new ArrayList<Long>();
        record.drainTo(points);
        Collections.sort(points);

        System.out.println("Data size :" + dataSize);
        System.out.println("Total Request : " + points.size());
        System.out.println("QPS(r/s) : " + 1000 * points.size() / taken);
        System.out.println("Total Thread : " + noThreads);
        System.out.println("P99 ms: " +  (double)percentile(points, 99) / 1000.0d);
        System.out.println("P99.5 ms: " +  (double)percentile(points, 99.5) / 1000.0d);
        System.out.println("P99.9 ms:" +  (double)percentile(points, 99.9) / 1000.0d);
    }

    public static long percentile(List<Long> latencies, double percentile) {
        int index = (int) Math.ceil(percentile / 100.0 * latencies.size());
        return latencies.get(index-1);
    }

    public static void main(String[] args) throws InterruptedException
    {
        //parse args
        CommandLineArgs cla = new CommandLineArgs();
        JCommander.newBuilder()
                .addObject(cla)
                .build()
                .parse(args);

        Benchmark benchmark = new Benchmark(cla.random, cla.noOps, cla.noConnections, cla.host, cla.port, cla.dataSize, cla.password, cla.mSize);
        benchmark.performBenchmark(cla.flag);
    }


}
