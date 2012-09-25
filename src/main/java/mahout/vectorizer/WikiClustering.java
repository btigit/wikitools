package mahout.vectorizer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.lucene.analysis.Analyzer;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.canopy.CanopyDriver;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.common.distance.TanimotoDistanceMeasure;
import org.apache.mahout.vectorizer.DictionaryVectorizer;
import org.apache.mahout.vectorizer.DocumentProcessor;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;

public class WikiClustering {
  
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
    int minSupport = 5;
    int minDf = 3;
    int maxDFPercent = 90;
    int maxNGramSize = 2;
    int minLLRValue = 50;
    int numReducers = 1;
    int chunkSizeInMegabytes = 50;
    int normPower = 2;
    boolean logNormalize = true;
    boolean sequentialAccessOutput = true;
    boolean namedVectors = false;
    
    String intputDir = "wiki/wiki-text";
    String outputDir = "wiki/wiki-vectors";
    
    Path vectorFolder = new Path(outputDir, "tfidf-vectors");
    Path canopyCentoroids = new Path(outputDir, "canopy-centroids");
    Path clusterOutput = new Path(outputDir, "clusters");
    
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    
    HadoopUtil.delete(conf, new Path(outputDir));
    
    tfidf(minSupport, minDf, maxDFPercent, maxNGramSize, minLLRValue,
        numReducers, chunkSizeInMegabytes, normPower, logNormalize,
        sequentialAccessOutput, namedVectors, intputDir, outputDir, conf);
    
    canopy(vectorFolder, canopyCentoroids, conf, fs);
  }

  private static void tfidf(int minSupport, int minDf, int maxDFPercent,
      int maxNGramSize, int minLLRValue, int numReducers,
      int chunkSizeInMegabytes, int normPower, boolean logNormalize,
      boolean sequentialAccessOutput, boolean namedVectors, String intputDir,
      String outputDir, Configuration conf) throws IOException,
      InterruptedException, ClassNotFoundException {
    
    //Tokenize Document
    Path tokenizedPath = new Path(outputDir,DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
    DocumentProcessor.tokenizeDocuments(new Path(intputDir), 
        JapaneseMahoutAnalyzer.class.asSubclass(Analyzer.class), tokenizedPath, conf);
    
    //TF
    DictionaryVectorizer.createTermFrequencyVectors(
        tokenizedPath, 
        new Path(outputDir), 
        DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER, 
        conf, 
        minSupport, 
        maxNGramSize, 
        minLLRValue, 
        -1.0f, 
        false, 
        numReducers, 
        chunkSizeInMegabytes, 
        sequentialAccessOutput, 
        namedVectors);
    
    //DF
    Pair<Long[], List<Path>> docFrequenciesFeatures = 
        TFIDFConverter.calculateDF(
              new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
              new Path(outputDir), 
              conf, 
              chunkSizeInMegabytes);

    //TF-IDF
    TFIDFConverter.processTfIdf(
        new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER), 
        new Path(outputDir), 
        conf, 
        docFrequenciesFeatures, 
        minDf, 
        maxDFPercent, 
        normPower, 
        logNormalize, 
        sequentialAccessOutput, 
        namedVectors, 
        numReducers);
  }

  private static void canopy(Path vectorFolder, Path canopyCentoroids, Configuration conf,
      FileSystem fs) throws IOException, InterruptedException,
      ClassNotFoundException {
    
    HadoopUtil.delete(conf, canopyCentoroids);
    
    CanopyDriver.run(
        conf,
        vectorFolder, 
        canopyCentoroids, 
        new EuclideanDistanceMeasure(), 
        0.1, 
        0.1, 
        false, 
        0, 
        false);
  }
  
  private static void kmeans(Path vectorFolder, Path canopyCentoroids, Path clusterOutput, Configuration conf,
      FileSystem fs) throws IOException, InterruptedException,
      ClassNotFoundException {
    
    KMeansDriver.run(
        conf, 
        vectorFolder, 
        new Path(canopyCentoroids, "clusters-0-final"), 
        clusterOutput, 
        new TanimotoDistanceMeasure(), 
        0.01, 
        20, 
        true, 
        0.0, 
        false);
    
    printCluster(conf, fs, new Path(clusterOutput, Cluster.CLUSTERED_POINTS_DIR+"/part-00000"));
  }

  private static void printCluster(Configuration conf, FileSystem fs, Path path) throws IOException {
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, path , conf);
    IntWritable key = new IntWritable();
    WeightedVectorWritable value = new WeightedVectorWritable();
    while (reader.next(key, value)) {
      System.out.println(key.toString()+" belongs to cluster "+value.toString());
    }
    reader.close();
  }
  
}
