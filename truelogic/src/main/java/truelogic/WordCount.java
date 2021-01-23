package truelogic;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class WordCount {
	public static void main(String[] args) {
		
		Myoptions options=PipelineOptionsFactory.fromArgs(args).withValidation().as(Myoptions.class);
		
	    Pipeline p = Pipeline.create(options);
	    p.apply(TextIO.read().from(options.getInputFile()))
	        .apply(
	            FlatMapElements.into(TypeDescriptors.strings())
	                .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
	        .apply(Filter.by((String word) -> !word.isEmpty()))
	        .apply(Count.perElement())
	        .apply(
	            MapElements.into(TypeDescriptors.strings())
	                .via(
	                    (KV<String, Long> wordCount) ->
	                        wordCount.getKey() + ": " + wordCount.getValue()))
	        .apply(TextIO.write().to(options.getOutputFile()).withNumShards(1).withSuffix(options.getExtn()));
	    p.run().waitUntilFinish();
	  }
}
