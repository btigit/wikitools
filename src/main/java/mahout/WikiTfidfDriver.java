package mahout;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikiTfidfDriver extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(WikiTfidfDriver.class);
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Configuration(), new WikiTfidfDriver(), args);
  }
  
  public int run(String[] args) throws Exception {
    addInputOption();
    addOutputOption();
    addOption(DefaultOptionCreator.distanceMeasureOption().create());
    return 0;
  }
  
}
