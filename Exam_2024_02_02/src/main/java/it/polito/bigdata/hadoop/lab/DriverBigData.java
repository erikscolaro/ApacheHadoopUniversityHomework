package it.polito.bigdata.hadoop.lab;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    Path inputPath;
    Path outputDir;
    Path outputDir2;
    int numberOfReducersJob1;
    int numberOfReducersJob2;
	  int exitCode;  
	
	// Parse the parameters
    numberOfReducersJob1 = Integer.parseInt(args[0]);
    numberOfReducersJob2 = Integer.parseInt(args[1]);
    inputPath = new Path(args[2]);
    outputDir = new Path(args[3]);
    outputDir2 = new Path(args[4]);

    Configuration conf = this.getConf();
    
    // Define a new job
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Exam20240219 - Hadoop");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job.setInputFormatClass();

    // Set job output format
    job.setOutputFormatClass();
       
    // Set map class
    job.setMapperClass(MapperBigData1.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass();
    job.setMapOutputValueClass();
    
    // Set reduce class
    job.setReducerClass(ReducerBigData1.class);
        
    // Set reduce output key and value classes
    job.setOutputKeyClass();
    job.setOutputValueClass();

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducersJob1);
    
    
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    {
        // Define a new job
        Job job2 = Job.getInstance(conf); 

        // Assign a name to the job
        job2.setJobName("Exam20240219 - Job 2");
        
        // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
        FileInputFormat.addInputPath(job2, outputDir);
        
        // Set path of the output folder for this job
        FileOutputFormat.setOutputPath(job2, outputDir2);
        
        // Specify the class of the Driver for this job
        job2.setJarByClass(DriverBigData.class);
        
        // Set job input format
        job2.setInputFormatClass();

        // Set job output format
        job2.setOutputFormatClass();
           
        // Set map class
        job2.setMapperClass(MapperBigData2.class);
        
        // Set map output key and value classes
        job2.setMapOutputKeyClass();
        job2.setMapOutputValueClass();
        
        // Set reduce class
        job2.setReducerClass(ReducerBigData2.class);
            
        // Set reduce output key and value classes
        job2.setOutputKeyClass();
        job2.setOutputValueClass();

        // Set number of reducers
        job2.setNumReduceTasks(numberOfReducersJob2);

        // Execute the job and wait for completion
        if (job2.waitForCompletion(true)==true)
        	exitCode=0;
       	else
       		exitCode=1;
    }
    else
    	exitCode=1;
    	
    return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}