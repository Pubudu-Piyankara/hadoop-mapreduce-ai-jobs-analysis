import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AIJobSkillAnalyzer {

    public static class SkillMapper extends Mapper<Object, Text, Text, IntWritable> {
        
        private final static IntWritable ONE = new IntWritable(1);
        private Text skillText = new Text();
        
        public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
            
            // Skip header row
            if (value.toString().contains("Job Title")) {
                return;
            }
            
            try {
                String[] columns = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
                
                if (columns.length >= 5) { // Assuming skills are in column 5
                    String skills = columns[4].replaceAll("\"", "").toLowerCase();
                    processSkills(skills, context);
                }
            } catch (Exception e) {
                context.getCounter("MAPPER_ERRORS", "EXCEPTIONS").increment(1);
            }
        }
        
        private void processSkills(String skills, Context context) 
                throws IOException, InterruptedException {
            
            // Split skills by common delimiters
            String[] skillList = skills.split("[,\\/;]");
            
            for (String skill : skillList) {
                String cleanedSkill = skill.trim();
                if (!cleanedSkill.isEmpty() && cleanedSkill.length() > 2) {
                    skillText.set(cleanedSkill);
                    context.write(skillText, ONE);
                }
            }
        }
    }

    public static class SkillReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        
        private IntWritable result = new IntWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context) 
                throws IOException, InterruptedException {
            
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "AI Job Skill Frequency");
        
        job.setJarByClass(AIJobSkillAnalyzer.class);
        job.setMapperClass(SkillMapper.class);
        job.setCombinerClass(SkillReducer.class);
        job.setReducerClass(SkillReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}