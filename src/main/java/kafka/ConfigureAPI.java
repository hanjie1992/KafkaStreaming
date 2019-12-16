package kafka;

public class ConfigureAPI
{

    public final static String GROUP_ID = "test3";

    public final static String TOPIC = "wangtianxiong";

    public final static int BUFFER_SIZE = 64 * 1024;

    public final static int TIMEOUT = 20000;

    public final static int INTERVAL = 10000;

    public final static String BROKER_LIST = "hadoop01:9092";
    public final static String CHROOT = "192.168.1.71:2181/kafka";

    // 去数据间隔
    public final static int GET_MEG_INTERVAL = 1000;

}