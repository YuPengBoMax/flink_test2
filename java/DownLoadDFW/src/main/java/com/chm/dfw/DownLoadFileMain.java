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

        //Instant startTime = Instant.now(); // ��ʼʱ��
        logger.info("--------------------" + DateUtil.getToday() + "--LOG--START------------------");

        // ��ȡ����·�� ���ڼ��������ļ�
        rootPath = System.getProperty("user.dir");

        //���������ļ�  ʧ�ܺ���ٳ���99��
        if(Config.getConfig(rootPath + "/guoyunDataConf/guoyunDataToLocationConfig.properties")){
            System.out.println("Load config successful!");
        }

        //��ȡ����ʱ��
        yesterday = DateUtil.getYesterday();
        //yesterday = "20191130";
        //��Ϸ��Ŀ
        int urlNum = Config.getUrls().size();

        // �����̳߳�
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(urlNum);

        // ������Ҫ���ص���Ϸ��Ŀ���̳߳��������
        for(index = 0; index < urlNum; index++) {
            fixedThreadPool.execute(new Runnable () {
                final int num = index;
                public void run () {
                    // ���ɳ�������5�Σ�ֱ���ɹ���
                    String url = Config.getUrls().get(num) + yesterday + ".log";
                    String fileName = url.split("/")[5];
                    for (int i =0; i<5; i++) {
                        if(DownLoadFile.downLoadStart( Config.getGameName(), url,
                                Config.getUserName(), Config.getPassword(), yesterday)){
                            System.out.println(yesterday + " " + Config.getGameName() + " DownLoad successfully��");
                            logger.info(yesterday + " " + Config.getGameName() + " DownLoad successfully��");
                            break;
                        }
                        if(i == 4) {   //���ǵ���4 ���ʾ��Ϸ����ʧ�� �����м�¼
                            System.out.println(yesterday + " " + Config.getGameName() + " DownLoad failed��");
                            logger.error(yesterday + " " + Config.getGameName() + " DownLoad failed��");
                        }
                        // ����ʧ�ܣ�����������֮ǰ��ɾ�����صĲ�ȱ����
                        DeleteFile.delAllFile(new File(Config.getFilePath() + Config.getGameName() + "/" + "prepared_log/" + yesterday+"/"+fileName));
                    }
                }
            });
        }
        // �ر��̳߳�
        fixedThreadPool.shutdown();

    }
}
