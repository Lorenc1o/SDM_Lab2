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

	public static List<Row> getTop10(GraphFrame gf, int nIter){
		System.out.println("\tPreparing PageRank...");
		PageRank pagerank =  gf.pageRank().resetProbability(0.15).maxIter(nIter);
		System.out.println("\tRunning PageRank...");
		GraphFrame rankedGraph = pagerank.run();

		System.out.println("\tDone! Sorting results...");
		Dataset<Row> ranks = rankedGraph.vertices().orderBy(functions.col("pagerank").desc());
		return ranks.select("id").limit(10).collectAsList();
	}

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

			Boolean converged = false;
			int iter = 1;
			int jump = 10;
			List<Row> left10 = null;
			List<Row> right10 = null;

			while(!converged){
				System.out.println("Iteration " + iter + "...");
				right10 = getTop10(gf, iter);

				if (left10 != null && left10.equals(right10)) {
					converged = true;
				} else {
					left10 = right10;
				}
				iter+=jump;
			}

			int left;
			if (iter >= 2*jump){
				left = iter-2*jump+1; // Because it could be to the left
			} else {
				left = iter-jump;
			}

			left10 = getTop10(gf, left);
			int right = iter;

			while (right - left > 1) {
				if(left == right-1){
					left = right;
				} else {
					System.out.println("The ideal number of iterations is between " + left + " and " + right);
					int midpoint = (left + right)/2; //this should be an integer
					List<Row> middle10 = getTop10(gf,midpoint);

					if(middle10.equals(left10)){
						right = left;
					} else if(middle10.equals(right10)){
						right = midpoint;
					} else {
						left = midpoint;
					}
				}
			}

			System.out.println("The ideal number of iterations is " + (left) + " and the obtained ranking is:");
			PageRank pagerank =  gf.pageRank().resetProbability(0.15).maxIter(left);
			GraphFrame rankedGraph = pagerank.run();
			Dataset<Row> ranks = rankedGraph.vertices().orderBy(functions.col("pagerank").desc());
			ranks.show(10);

		} catch (IOException e){
			e.printStackTrace();
		}
	}

}
