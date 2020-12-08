package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 批处理中使用 Field Expressions 来指定key，使用groupBy:
 *   1.对于类对象可以使用类中的字段来指定key
 *   2.对于嵌套的tuple类型的tuple数据可以指定嵌套tuple中某个位置的数据当做key
 *
 * socket测试数据：
 *  xiaoming	18	m	2	1	2	3
 *  xiaohong	19	f	1	4	5	3
 *  xiaoli	20	m	1	7	8	9
 */
public class KeyByUseFieldExpressions {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> socketText = env.socketTextStream("mynode5", 9999);
        SingleOutputStreamOperator<StudentsInfo> map = socketText.map(new MapFunction<String, StudentsInfo>() {
            @Override
            //xiaoming	18	m	2	1	2	3
            public StudentsInfo map(String line) throws Exception {
                String name = line.split("\t")[0];
                Integer age = Integer.valueOf(line.split("\t")[1]);
                String gender = line.split("\t")[2];
                Integer grade = Integer.valueOf(line.split("\t")[3]);
                Float chinese = Float.valueOf(line.split("\t")[4]);
                Float math = Float.valueOf(line.split("\t")[5]);
                Float english = Float.valueOf(line.split("\t")[6]);
                return new StudentsInfo(name, age, gender, new Tuple2<Integer, Tuple3<Float, Float, Float>>(grade, new Tuple3<>(chinese, math, english)));
            }
        });

        //使用类中的字段来指定key
//        KeyedStream<StudentsInfo, Tuple> groupBy = map.keyBy("gender");
        //嵌套的tuple2类型中Tuple3的第三位“英语成绩”数据为指定key
        KeyedStream<StudentsInfo, Tuple> groupBy = map.keyBy("gradeAndScores.1.2");
//        KeyedStream<StudentsInfo, Tuple> groupBy = map.keyBy("gradeAndScores.f1.f2");

        WindowedStream<StudentsInfo, Tuple, TimeWindow> timeWindow = groupBy.timeWindow(Time.seconds(5));
        SingleOutputStreamOperator<StudentsInfo> outputStream = timeWindow.reduce(new ReduceFunction<StudentsInfo>() {
            @Override
            public StudentsInfo reduce(StudentsInfo si1, StudentsInfo si2) throws Exception {
                return new StudentsInfo(si1.name + si2.name,
                        si1.age + si2.age,
                        si1.gender + si2.gender,
                        // private Tuple2<Integer,Tuple3<Float,Float,Float>> gradeAndScores;
                        new Tuple2<Integer, Tuple3<Float, Float, Float>>(si1.gradeAndScores.f0 + si2.gradeAndScores.f0,
                                new Tuple3(si1.gradeAndScores.f1.f0 + si2.gradeAndScores.f1.f0,
                                        si1.gradeAndScores.f1.f1 + si2.gradeAndScores.f1.f1,
                                        si1.gradeAndScores.f1.f2 + si2.gradeAndScores.f1.f2))
                );
            }
        });

        outputStream.print();
        env.execute("keyBy-FieldExpressions");

    }

    /**
     * 自定义类需要注意：
     * 1.类的访问级别必须是public
     * 2.必须写出默认的空的构造函数
     * 3.类中所有字段是public类型或者实现了getter和setter方法
     * 4.Flink必须支持字段的类型。一般类型都支持
     */
    public static class StudentsInfo{
        private String name;//姓名
        private Integer age;//年龄
        private String gender;//性别
        //年级grade，语chinese数math外english成绩
        private Tuple2<Integer,Tuple3<Float,Float,Float>> gradeAndScores;

        public StudentsInfo() {
        }

        public StudentsInfo(String name, Integer age, String gender, Tuple2<Integer, Tuple3<Float, Float, Float>> gradeAndScores) {
            this.name = name;
            this.age = age;
            this.gender = gender;
            this.gradeAndScores = gradeAndScores;
        }


        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }

        public String getGender() {
            return gender;
        }
        public Tuple2<Integer, Tuple3<Float, Float, Float>> getGradeAndScores() {
            return gradeAndScores;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public void setGradeAndScores(Tuple2<Integer, Tuple3<Float, Float, Float>> gradeAndScores) {
            this.gradeAndScores = gradeAndScores;
        }

        @Override
        public String toString() {
            return "StudentsInfo{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", gender='" + gender + '\'' +
                    ", gradeAndScores=" + gradeAndScores +
                    '}';
        }

    }

}
