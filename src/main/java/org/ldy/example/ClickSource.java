package org.ldy.example;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Calendar;
import java.util.Random;
public class ClickSource implements SourceFunction<Event> {
    // 声明一个布尔变量，作为控制数据生成的标识位
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        Random random = new Random(); // 在指定的数据集中随机选取数据
        String[] users = {"Mary", "Alice", "Bob", "Cary"};
        String[] urls = {"./home", "./cart", "./fav", "./prod?id=1",
                "./prod?id=2"};
        while (running) {
            ctx.collect(new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            // 隔 1 秒生成一个点击事件，方便观测
            Thread.sleep(1000);
        }
    }
    @Override
    public void cancel() {
        running = false;
    }
}

//        import org.apache.flink.streaming.api.datastream.DataStreamSource;
//        import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//public class SourceCustom {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env =
//                StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
////有了自定义的 source function，调用 addSource 方法
//        DataStreamSource<Event> stream = env.addSource(new ClickSource());
//        stream.print("SourceCustom");
//        env.execute();
//    }
//
//}
