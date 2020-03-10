package com.chm.flink.wordCount;



/**
 * @ClassName:  WordWithCount
 * @Description:TODO(这里用一句话描述这个类的作用)
 * @author: Jimu
 * @email:  maker2win@163.com
 * @date:   2019年3月19日 下午3:26:52
 *
 * @Copyright: 2019 www.maker-win.net Inc. All rights reserved.
 *
 */
public class WordWithCount {
    public String word;
    public long count;

    public WordWithCount() {}

    public WordWithCount(String word, long count) {
        this.word = word;
        this.count = count;
    }

    @Override
    public String toString() {
        return word + " : " + count;
    }
}

