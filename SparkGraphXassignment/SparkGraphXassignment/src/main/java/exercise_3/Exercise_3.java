package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashMap;



public class Exercise_3 {

    // Stores the predecessor of each vertex on the shortest path
    static Map<Object, Object> predecessors = new HashMap<Object, Object>();

    // function to recursively get the path until the source vertex
    public static void get_recursive_path(Object vert, Map<Object, Object> p_map, List<String> path, Map<Long, String> labels){
        if (p_map.get(vert)==(Object)(-1l)) { //source vertex
            return;
        } else {
            //get parent and add to path
            Object parent_vertex = p_map.get(vert);
            path.add(labels.get(parent_vertex));
            //call again with parent to get path to the parent
            get_recursive_path(parent_vertex, p_map, path, labels);
        }
    }


    // get the shortest path as list
    public static List<String> get_path(Object vertex, Map<Object, Object> p_map, Map<Long, String> labels){
        List<String> path = new ArrayList<>();
        path.add(labels.get(vertex));
        //get the shortest path
        get_recursive_path(vertex, p_map, path, labels);

        // Reverse the path list (to get correct output order)
        for (int k = 0, j = path.size() - 1; k < j; k++)
        {
            path.add(k, path.remove(j));
        }
        return path;
    }

    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            if (vertexValue == 0) { // start vertex A
                return 0;
            } else if (vertexValue < message) { // no change of vertex value needed
                return vertexValue;
            } else { // update vertex value
                return message;

            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {

            Integer path_len = 0;
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();

            if(Integer.MAX_VALUE == sourceVertex._2){ //source value has still infinity value
                path_len = Integer.MAX_VALUE;
            } else{ //calculate path cost
                path_len = triplet.attr + sourceVertex._2;
            }


            if (path_len < dstVertex._2) {  //shorter path found
                // update the predecessor
                predecessors.put(dstVertex._1, sourceVertex._1);
                // propagate value
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),path_len)).iterator()).asScala();
            } else {
                // do nothing
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            }
        }
    }

    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Integer o, Integer o2) {
            return null;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        //already initiliazed for shortest path alg for node A
        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
                new Tuple2<Object,Integer>(1l,0),
                new Tuple2<Object,Integer>(2l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(3l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(4l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(5l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(6l,Integer.MAX_VALUE)
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        // initialize the predecessors (hash map size 6 for every vertice the id and -1 because we dont now the predecessor yet)
        for (Tuple2<Object,Integer> vertex_var :vertices){
            Object key = vertex_var._1();
            predecessors.put(key, (Object)(-1l));
        }

        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        ops.pregel(Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new Exercise_3.VProg(),
                        new Exercise_3.sendMsg(),
                        new Exercise_3.merge(),
                        ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .toJavaRDD().sortBy(f -> ((Tuple2<Object, Integer>) f)._1, true, 0)
                .foreach(v -> {
                    Tuple2<Object,Integer> vertex = (Tuple2<Object,Integer>)v;
                    // get the shortest path
                    List<String> path = get_path( vertex._1 , predecessors, labels);
                    System.out.println("Minimum path to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is " + path + " with cost " +vertex._2);
                });
    }

}
