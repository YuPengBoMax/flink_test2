import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author  ypb
 */
public class DownLoadFileMain {

    private  static int index;
    private  static String yesterday;
    private  static String rootPath;
    private static Logger logger = LogManager.getLogger(DownLoadFileMain.class);

    public static void main(String[] args) {

        //Instant startTime = Instant.now(); // 开始时间
        logger.info("--------------------" + DateUtil.getToday() + "--LOG--START------------------");

        // 获取程序路径 用于加载配置文件
        rootPath = System.getProperty("user.dir");

        //加载配置文件  失败后可再尝试99次
        if(Config.getConfig(rootPath + "/guoyunDataConf/guoyunDataToLocationConfig.properties")){
            System.out.println("Load config successful!");
        }

        //获取昨日时间
        yesterday = DateUtil.getYesterday();
        //yesterday = "20191130";
        //游戏数目
        int urlNum = Config.getUrls().size();

        // 定义线程池
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(urlNum);

        // 按照需要下载的游戏数目向线程池添加任务
        for(index = 0; index < urlNum; index++) {
            fixedThreadPool.execute(new Runnable () {
                final int num = index;
                public void run () {
                    // 最多可尝试下载5次，直到成功。
                    String url = Config.getUrls().get(num) + yesterday + ".log";
                    String fileName = url.split("/")[5];
                    for (int i =0; i<5; i++) {
                        if(DownLoadFile.downLoadStart( Config.getGameName(), url,
                                Config.getUserName(), Config.getPassword(), yesterday)){
                            System.out.println(yesterday + " " + Config.getGameName() + " DownLoad successfully！");
                            logger.info(yesterday + " " + Config.getGameName() + " DownLoad successfully！");
                            break;
                        }
                        if(i == 4) {   //若是等于4 则表示游戏下载失败 ，进行记录
                            System.out.println(yesterday + " " + Config.getGameName() + " DownLoad failed！");
                            logger.error(yesterday + " " + Config.getGameName() + " DownLoad failed！");
                        }
                        // 下载失败，在重新下载之前，删除下载的残缺数据
                        DeleteFile.delAllFile(new File(Config.getFilePath() + Config.getGameName() + "/" + "prepared_log/" + yesterday+"/"+fileName));
                    }
                }
            });
        }
        // 关闭线程池
        fixedThreadPool.shutdown();

    }
}
