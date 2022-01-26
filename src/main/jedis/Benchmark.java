package jedis;

import com.beust.jcommander.JCommander;
import org.apache.commons.lang3.RandomStringUtils;
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

            for(int i = 0; i < noOps_; i++) {

                String msetArgs[] = new String[msize * 2];
                for(int j = 0; j < msize * 2; ){
                    String key = String.valueOf(new Random().nextInt(r));
                    String value = RandomStringUtils.random(dataSize);
                    msetArgs[j] = key;
                    msetArgs[j+1] = value;
                    j = j + 2;
                }

                long startTime = System.nanoTime();
                jedis.mset(msetArgs);
                setRunTimes.offer(System.nanoTime() - startTime);
                setLatch.countDown();
            }
            pool.returnResource(jedis);
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


            for(int j = 0; j < noOps_; j++) {
                long startTime = System.nanoTime();

                Jedis jedis = pool.getResource();
                String mgetArgs[] = new String[msize];
                for (int i = 0; i < msize; i++) {
                    String key = String.valueOf(new Random().nextInt(r));
                    mgetArgs[i] = key;
                }
                jedis.mget(mgetArgs);
                getRunTimes.offer(System.nanoTime() - startTime);
                getLatch.countDown();

                pool.returnResource(jedis);
            }

         }
    }


    public void performBenchmark() throws InterruptedException
    {

        long start = System.nanoTime();
        Executor executor = Executors.newFixedThreadPool(noThreads);
        for (int i = 0; i < noThreads; i++) {
           int cnt = noOps_ / noThreads;
           new MSetThread("set thread").start();
        }
        setLatch.await();
        long totalNanoRunTime = System.nanoTime() - start;
        System.out.println("-------------- mset -----------------");
        printStats(totalNanoRunTime, setRunTimes);


        start = System.nanoTime();
        for (int i = 0; i < noThreads; i++) {
            int cnt = noOps_ / noThreads;
            new MGetThread("get thread").start();
        }
        getLatch.await();
        totalNanoRunTime = System.nanoTime() - start;
        System.out.println("-------------- mget -----------------");
        printStats(totalNanoRunTime, getRunTimes);
    }


    public void printStats(long totalNanoRunTime, LinkedBlockingQueue<Long>  record)
    {
        List<Long> points = new ArrayList<Long>();
        record.drainTo(points);
        Collections.sort(points);
        long sum = 0;
        for (Long l : points)
        {
            sum += l;
        }

        System.out.println("Data size :" + dataSize);
        System.out.println("Total Request : " + points.size());
        System.out.println("Time Test Run for (ms) : " + TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime));
        double avg = (double)TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime) / (double)points.size();
        System.out.println("Average(ms): " + avg);
        System.out.println("50 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() / 2) - 1)));
        System.out.println("90 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 90 / 100) - 1)));
        System.out.println("95 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 95 / 100) - 1)));
        System.out.println("99 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 99 / 100) - 1)));
        System.out.println("99.9 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get((points.size() * 999 / 1000) - 1)));
        System.out.println("100 % <=" + TimeUnit.NANOSECONDS.toMillis(points.get(points.size() - 1)));
        System.out.println((points.size() * 1000 / TimeUnit.NANOSECONDS.toMillis(totalNanoRunTime)) + " Operations per second");
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
        benchmark.performBenchmark();
    }


}
