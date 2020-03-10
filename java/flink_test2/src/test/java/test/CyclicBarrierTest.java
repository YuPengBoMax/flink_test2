package test;


import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @Description
 * @author 大都督
 * @date 2019年5月1日
 */
public class CyclicBarrierTest {

    //内部类
    public class PersonThread extends Thread{
        private CyclicBarrier cyclicBarrier;//私有实例变量
        public PersonThread(CyclicBarrier cyclicBarrier) {//构造函数
            this.cyclicBarrier = cyclicBarrier;
        }
        @Override
        public void run() {//重写run方法
            System.out.println(Thread.currentThread().getName() + "，来了");
            try {
                //模拟子线程需要处理的业务
                Thread.sleep(3000);
            } catch (Exception e) {
                // TODO: handle exception
            }
            System.out.println( Thread.currentThread().getName() + ",准备好了");
            try {
                //只有人都到齐了，才能进行到下一步
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            System.out.println(Thread.currentThread().getName() + "万事俱备，放火，烧赤壁");
        }
    }

    static         CyclicBarrier cyclicBarrier = new CyclicBarrier(4);


    public static void main(String[] args) {
        CyclicBarrierTest cyclicBarrierTest = new CyclicBarrierTest();
        //假设：火烧赤壁需要三个人：周瑜（下令放火），鲁肃（请诸葛亮），诸葛亮（借东风）
    /*    CyclicBarrier cyclicBarrier = new CyclicBarrier(3, new Runnable() {
            @Override
            public void run() {
                System.out.println("三个人到齐后，所有其他线程被唤醒前执行");
            }
        });*/
        PersonThread zhouyu = cyclicBarrierTest.new PersonThread(cyclicBarrier);
        zhouyu.setName("周瑜");
        zhouyu.start();
        PersonThread lusu = cyclicBarrierTest.new PersonThread(cyclicBarrier);
        lusu.setName("鲁肃");
        lusu.start();
        PersonThread zhugeliang = cyclicBarrierTest.new PersonThread(cyclicBarrier);
        Thread thread = new Thread() {
            @Override
            public void run() {
                if (!cyclicBarrier.isBroken()) {
                    try {
                        Thread.sleep(3000);
                        cyclicBarrier.await();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(cyclicBarrier.isBroken());
                }
            }
        };
        thread.start();
        zhugeliang.setName("诸葛亮");
        zhugeliang.start();
    }

}

