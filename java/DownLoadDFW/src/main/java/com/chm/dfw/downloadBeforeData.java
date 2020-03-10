package com.chm.dfw;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class downloadBeforeData {

    private static String rootPath;
    private static Logger logger = LogManager.getLogger(downloadBeforeData.class);


    public static void main(String[] args) {
        //String start = "20190621";
        //String end = "20190701";
        String start = args[0];
        String end = args[1];

        //Instant startTime = Instant.now(); // ��ʼʱ��
        logger.info("---------" + DateUtil.getToday() + "--LOG--START-------------");

        // ��ȡ����·�� ���ڼ��������ļ�
        rootPath = System.getProperty("user.dir");

        if (Config.getConfig(rootPath + "/guoyunDataConf/guoyunDataDFWToLocationConfig.properties")) {
            System.out.println("Load Config successful!");
        }
        //��ȡ����ʱ��
        int urlNum = Config.getUrls().size();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(urlNum);


        Calendar calendar = DateUtil.getStartCalendar(start);
        List<String> dates = new ArrayList<String>();
        while (true) {
            dates.add(start);
            System.out.println(start);
            start = DateUtil.getNextDay(calendar);
            if (start.equals(end)) {
                break;
            }
        }

        for (String startdate : dates) {
            for (int index = 0; index < urlNum; index++) {
                RunableTask task = new RunableTask(startdate,index);
                fixedThreadPool.execute(task);
            }
        }


        // �ر��̳߳�
        fixedThreadPool.shutdown();
    }
}
