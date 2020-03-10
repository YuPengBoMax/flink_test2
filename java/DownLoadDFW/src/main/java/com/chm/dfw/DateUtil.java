package com.chm.dfw;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;


public class DateUtil {

    /**
     *
	 * @return  昨天时间
	 */
	public static String getYesterday() {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY,-24);
		String yesterdayDate = dateFormat.format(calendar.getTime());
		return yesterdayDate;		
	}

    /**
	 * @return  今天时间
 	 */
	public static String getToday() {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		String todayDate = dateFormat.format(calendar.getTime());
		return todayDate;		
	}

	public static String getNextDay(Calendar cal) {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = cal;
		calendar.set(Calendar.HOUR_OF_DAY,+24);
		String date = dateFormat.format(calendar.getTime());
		return date;
	}

	public static Calendar getStartCalendar(String startDate) {
		DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		Calendar calendar = Calendar.getInstance();
		String yesterdayDate = "";
		while(true) {
			calendar.set(Calendar.HOUR_OF_DAY, -24);
			yesterdayDate = dateFormat.format(calendar.getTime());
			if(yesterdayDate.equals(startDate)){
				break;
			}
		}
		return calendar;
	}


}
