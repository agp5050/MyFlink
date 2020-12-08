package com.lw.myflink.SpecifyingKeys;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 *  批处理中使用 Field Expressions 来指定key，使用groupBy:
 *  1.对于类对象可以使用类中的字段来指定key
 *  2.对于嵌套的tuple类型的tuple数据可以指定嵌套tuple中某个位置的数据当做key
 *
 */
public class GroupByUseFieldExpressions {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("./data/studentsScoreInfo");
        MapOperator<String, StudentsInfo> map = dataSource.map(new MapFunction<String, StudentsInfo>() {
            @Override
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
//        UnsortedGrouping<StudentsInfo> groupBy = map.groupBy("gender");
        //嵌套的tuple2类型中Tuple3的第三位“英语成绩”数据为指定key
//        UnsortedGrouping<StudentsInfo> groupBy = map.groupBy("gradeAndScores.1.2");
        UnsortedGrouping<StudentsInfo> groupBy = map.groupBy("gradeAndScores.f1.f2");
        GroupReduceOperator<StudentsInfo, StudentsInfo> groupReduce = groupBy.reduceGroup(new GroupReduceFunction<StudentsInfo, StudentsInfo>() {
            @Override
            public void reduce(Iterable<StudentsInfo> iterable, Collector<StudentsInfo> collector) throws Exception {
                System.out.println("new key ... ... ");
                String name = "";
                Integer age = 0;
                String gender = "";
                Integer grade = 0;
                Float chinese = 0.0f;
                Float math = 0.0f;
                Float english = 0.0f;
                for (StudentsInfo studentsInfo : iterable) {
                    System.out.println(studentsInfo);
                    name += studentsInfo.getName();
                    age += studentsInfo.getAge();
                    gender += studentsInfo.getGender();
                    grade += studentsInfo.getGradeAndScores().f0;
                    chinese += studentsInfo.getGradeAndScores().f1.f0;
                    math += studentsInfo.getGradeAndScores().f1.f1;
                    english += studentsInfo.getGradeAndScores().f1.f2;

                }
                StudentsInfo si = new StudentsInfo(name, age, gender, new Tuple2<Integer, Tuple3<Float, Float, Float>>(grade, new Tuple3<>(chinese, math, english)));
                collector.collect(si);
            }
        });

        groupReduce.print();
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
