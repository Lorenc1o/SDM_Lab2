package exercise_4;

import com.clearspring.analytics.util.Lists;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;

import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		// 1. Load the Data
		try{
			// Vertices creation
			java.util.List<Row> vertices_list = new ArrayList<Row>();

			String line = null;
			FileReader fileReader = new FileReader("src/main/resources/wiki-vertices.txt");
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			while((line = bufferedReader.readLine()) != null){
				String parts[] = line.split("\t", 2);
				String id = parts[0];
				String title = parts[1];

				vertices_list.add(RowFactory.create(id,title));
			}

			JavaRDD<Row> vertices_rdd = ctx.parallelize(vertices_list);

			StructType vertices_schema = new StructType(new StructField[]{
					new StructField("id", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> vertices =  sqlCtx.createDataFrame(vertices_rdd, vertices_schema);

			fileReader.close();
			bufferedReader.close();

			// Edges creation
			java.util.List<Row> edges_list = new ArrayList<Row>();

			fileReader = new FileReader("src/main/resources/wiki-edges.txt");
			bufferedReader = new BufferedReader(fileReader);

			while((line = bufferedReader.readLine()) != null){
				String parts[] = line.split("\t", 2);
				String src = parts[0];
				String dst = parts[1];

				edges_list.add(RowFactory.create(src, dst, "References"));
			}

			JavaRDD<Row> edges_rdd = ctx.parallelize(edges_list);

			StructType edges_schema = new StructType(new StructField[]{
					new StructField("src", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("dst", DataTypes.StringType, true, new MetadataBuilder().build()),
					new StructField("relationship", DataTypes.StringType, true, new MetadataBuilder().build())
			});

			Dataset<Row> edges = sqlCtx.createDataFrame(edges_rdd, edges_schema);

			fileReader.close();
			bufferedReader.close();

			// Create the graph
			GraphFrame gf = GraphFrame.apply(vertices,edges);

			System.out.println(gf);

			gf.edges().show();
			gf.vertices().show();

			System.out.println("Preparing PageRank...");
			PageRank pagerank =  gf.pageRank().resetProbability(0.15).maxIter(5);
			System.out.println("Running PageRank...");
			GraphFrame rankedGraph = pagerank.run();

			System.out.println("Done! Sorting results...");
			Dataset<Row> ranks = rankedGraph.vertices().orderBy(functions.col("pagerank").desc());

			ranks.show(10);

		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
}
