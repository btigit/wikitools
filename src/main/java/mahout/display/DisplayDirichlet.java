/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mahout.display;

import java.awt.Graphics;
import java.awt.Graphics2D;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.Model;
import org.apache.mahout.clustering.ModelDistribution;
import org.apache.mahout.clustering.classify.ClusterClassifier;
import org.apache.mahout.clustering.dirichlet.DirichletDriver;
import org.apache.mahout.clustering.dirichlet.models.DistributionDescription;
import org.apache.mahout.clustering.dirichlet.models.GaussianClusterDistribution;
import org.apache.mahout.clustering.display.DisplayClustering;
import org.apache.mahout.clustering.iterator.ClusterIterator;
import org.apache.mahout.clustering.iterator.DirichletClusteringPolicy;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.RandomUtils;
import org.apache.mahout.common.distance.ManhattanDistanceMeasure;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

public class DisplayDirichlet extends DisplayClustering {
  
  public DisplayDirichlet() {
    initialize();
  }
  
  // Override the paint() method
  @Override
  public void paint(Graphics g) {
    plotSampleData((Graphics2D) g);
    plotClusters((Graphics2D) g);
  }
  
  protected static void generateResults(Path output,
      ModelDistribution<VectorWritable> modelDist, int numClusters, int numIterations, double alpha0, int thin, int burnin) throws IOException, ClassNotFoundException,
      InterruptedException {
    for (int i = 1; i <= numIterations; i++) {
      ClusterClassifier posterior = new ClusterClassifier();
      String name = i == numIterations ? "clusters-" + i + "-final" : "clusters-" + i;
      posterior.readFromSeqFiles(new Configuration(), new Path(output, name));
      List<Cluster> clusters = Lists.newArrayList();
      for (Cluster cluster : posterior.getModels()) {
        if (isSignificant(cluster)) {
          clusters.add(cluster);
        }
      }
      CLUSTERS.add(clusters);
    }
  }
  
  public static void main(String[] args) throws Exception {
    VectorWritable modelPrototype = new VectorWritable(new DenseVector(2));
    ModelDistribution<VectorWritable> modelDist = new GaussianClusterDistribution(modelPrototype);
    Configuration conf = new Configuration();
    Path output = new Path("output");
    int numIterations = 20;
    int numClusters = 10;
    int alpha0 = 1;
    int thin = 3;
    int burnin = 5;
    generateResults(output, modelDist, numClusters, numIterations, alpha0, thin, burnin);
    new DisplayDirichlet();
  }
  
}
