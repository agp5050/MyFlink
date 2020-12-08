package com.lw.myflink.Streaming.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.*;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;


public class MyTransactionSink extends TwoPhaseCommitSinkFunction<String, MyTransactionSink.ContentTransaction, Void> {

    private ContentBuffer contentBuffer = new ContentBuffer();

    public MyTransactionSink() {
        super(new KryoSerializer<>(ContentTransaction.class, new ExecutionConfig()),VoidSerializer.INSTANCE);
    }

    @Override
    /**
     * 当有数据时，会执行到这个invoke方法
     * 2执行
     */
    protected void invoke(ContentTransaction transaction, String value, Context context) throws InterruptedException {
        System.out.println("====invoke===="+value);
        transaction.tmpContentWriter.write(value);
        System.out.println("pre-Commit----正在休息5s");
        Thread.sleep(5000);
        System.out.println("pre-Commit----休息完成5s");

    }

    @Override
    /**
     * 开启一个事务，在临时目录下创建一个临时文件，之后，写入数据到该文件中
     * 1执行
     */
    protected ContentTransaction beginTransaction() {
        ContentTransaction contentTransaction= new ContentTransaction(contentBuffer.createWriter(UUID.randomUUID().toString()));
        System.out.println("====beginTransaction====,contentTransaction Name = "+contentTransaction.toString());
//        return new ContentTransaction(tmpDirectory.createWriter(UUID.randomUUID().toString()));
        return contentTransaction;
    }

    @Override
    /**
     * 在pre-commit阶段，flush缓存数据块到磁盘，然后关闭该文件，确保再不写入新数据到该文件。
     *  同时开启一个新事务执行属于下一个checkpoint的写入操作
     * 3执行
     */
    protected void preCommit(ContentTransaction transaction) {
        System.out.println("====preCommit====,contentTransaction Name = "+transaction.toString());

        transaction.tmpContentWriter.flush();
        transaction.tmpContentWriter.close();


    }

    @Override
    /**
     * 在commit阶段，我们以原子性的方式将上一阶段的文件写入真正的文件目录下。这里有延迟
     * 4执行
     */
    protected void commit(ContentTransaction transaction) {
        System.out.println("====commit====,contentTransaction Name = "+transaction.toString());

        /**
         * 实现写入文件的逻辑
         */
        //获取名称
        String name = transaction.tmpContentWriter.getName();
        //获取数据
        Collection<String> content = contentBuffer.read(name);

        /**
         * 测试打印
         */
        for(String s: content){
//            if("hello-1".equals(s)){
//                try {
//                    System.out.println("正在休息5s");
//                    Thread.sleep(5000);
//                    System.out.println("休息完成5s");
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            System.out.println(s);
        }

//        //将数据写入文件
//        FileWriter fw =null;
//        PrintWriter pw =null ;
//        try {
//            //如果文件存在，则追加内容；如果文件不存在，则创建文件
//            File dir=new File("./data/FileResult/result.txt");
//            if(!dir.getParentFile().exists()){
//                dir.getParentFile().mkdirs();//创建父级文件路径
//                dir.createNewFile();//创建文件
//            }
//            fw = new FileWriter(dir, true);
//            pw = new PrintWriter(fw);
//            for(String s:content){
//                if(s.equals("sss-1")){
//                    throw new NullPointerException();
//                }
//                pw.write(s+"\n");
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        try {
//            pw.flush();
//            fw.flush();
//            pw.close();
//            fw.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    /**
     * 一旦有异常终止事务时，删除临时文件
     */
    protected void abort(ContentTransaction transaction) {
        System.out.println("====abort====");
        transaction.tmpContentWriter.close();
        contentBuffer.delete(transaction.tmpContentWriter.getName());
    }

    public static class ContentTransaction {
        private ContentBuffer.TempContentWriter tmpContentWriter;

        public ContentTransaction(ContentBuffer.TempContentWriter tmpContentWriter) {
            this.tmpContentWriter = tmpContentWriter;
        }

        @Override
        public String toString() {
            return String.format("ContentTransaction[%s]", tmpContentWriter.getName());
        }
    }

}

/**
 * ContentBuffer 类中 放临时的处理数据到一个list中
 */
class ContentBuffer implements Serializable {
    private Map<String, List<String>> filesContent = new HashMap<>();

    public TempContentWriter createWriter(String name) {
        checkArgument(!filesContent.containsKey(name), "File [%s] already exists", name);
        filesContent.put(name, new ArrayList<>());
        return new TempContentWriter(name, this);
    }
    private void putContent(String name, List<String> values) {
        List<String> content = filesContent.get(name);
        checkState(content != null, "Unknown file [%s]", name);
        content.addAll(values);
    }

    public Collection<String> read(String name) {
        List<String> content = filesContent.get(name);
        checkState(content != null, "Unknown file [%s]", name);
        List<String> result = new ArrayList<>(content);
        return result;
    }

    public void delete(String name) {
        filesContent.remove(name);
    }

    //内部类
    class TempContentWriter {
        private final ContentBuffer contentBuffer;
        private final String name;
        private final List<String> buffer = new ArrayList<>();
        private boolean closed = false;

        public String getName() {
            return name;
        }

        private TempContentWriter(String name, ContentBuffer contentBuffer) {
            this.name = checkNotNull(name);
            this.contentBuffer = checkNotNull(contentBuffer);
        }

        public TempContentWriter write(String value) {
            checkState(!closed);
            buffer.add(value);
            return this;
        }

        public TempContentWriter flush() {
            contentBuffer.putContent(name, buffer);
            return this;
        }

        public void close() {
            buffer.clear();
            closed = true;
        }
    }

}

