package com.lw.myflink.Streaming.kafka;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.UUID;


public class MyTransactionSink_bak extends TwoPhaseCommitSinkFunction<String, MyTransactionSink_bak.ContentTransaction, Void> {


    private ContentDump tmpDirectory =  new ContentDump();
//    private ContentDump targetDirectory = new ContentDump();


    public MyTransactionSink_bak() {
        super(new KryoSerializer<>(ContentTransaction.class, new ExecutionConfig()),VoidSerializer.INSTANCE);
    }

    @Override
    /**
     * 当有数据时，会执行到这个invoke方法
     * 2执行
     */
    protected void invoke(ContentTransaction transaction, String value, Context context) throws Exception {
        System.out.println("====invoke===="+value);
        transaction.tmpContentWriter.write(value);

    }

    @Override
    /**
     * 开启一个事务，在临时目录下创建一个临时文件，之后，写入数据到该文件中
     * 1执行
     */
    protected ContentTransaction beginTransaction() throws Exception {
        ContentTransaction contentTransaction= new ContentTransaction(tmpDirectory.createWriter(UUID.randomUUID().toString()));
        System.out.println("====beginTransaction====,contentTransaction Name = "+contentTransaction.toString());
//        return new ContentTransaction(tmpDirectory.createWriter(UUID.randomUUID().toString()));
        return contentTransaction;
    }

    @Override
    /**
     * 在pre-commit阶段，flush缓存数据块到磁盘，然后关闭该文件，确保再不写入新数据到该文件。同时开启一个新事务执行属于下一个checkpoint的写入操作
     * 3执行
     */
    protected void preCommit(ContentTransaction transaction) throws Exception {
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
        Collection<String> content = tmpDirectory.read(name);
        //将数据写入文件：这里先暂时打印
//        for(String s:content){
//            System.out.println(s);
//        }

        FileWriter fw = null;
        try {
            //如果文件存在，则追加内容；如果文件不存在，则创建文件
            File dir=new File("./data/file/allFile/"+name+".txt");
//            if(!dir.getParentFile().exists()){
//                dir.getParentFile().mkdirs();//创建父级文件路径
//                dir.createNewFile();//创建文件
//            }
            fw = new FileWriter(dir, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        PrintWriter pw = new PrintWriter(fw);
        for(String s:content){
            pw.write(s+"\n");
        }
        pw.flush();
        try {
            fw.flush();
            pw.close();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }



//        ContentDump.move(
//                transaction.tmpContentWriter.getName(),
//                tmpDirectory,
//                targetDirectory);

    }

    @Override
    /**
     * 一旦有异常终止事务时，删除临时文件
     */
    protected void abort(ContentTransaction transaction) {
        System.out.println("====abort====");
        transaction.tmpContentWriter.close();
        tmpDirectory.delete(transaction.tmpContentWriter.getName());
    }

    public static class ContentTransaction {
        private ContentDump.ContentWriter tmpContentWriter;

        public ContentTransaction(ContentDump.ContentWriter tmpContentWriter) {
            this.tmpContentWriter = tmpContentWriter;
        }

        @Override
        public String toString() {
            return String.format("ContentTransaction[%s]", tmpContentWriter.getName());
        }
    }

}