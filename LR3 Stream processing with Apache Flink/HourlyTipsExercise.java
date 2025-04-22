package com.ververica.flinktraining.exercises.datastream_java.windows;
import com.ververica.flinktraining.exercises.datastream_java.datatypes.TaxiFare;
import com.ververica.flinktraining.exercises.datastream_java.sources.TaxiFareSource;
import com.ververica.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HourlyTipsExercise extends ExerciseBase {

	public static void main(String[] args) throws Exception {

		// read parameters
		ParameterTool params = ParameterTool.fromArgs(args);
		final String input = params.get("input", ExerciseBase.pathToFareData);

		final int maxEventDelay = 60;       // events are out of order by max 60 seconds
		final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(ExerciseBase.parallelism);

		DataStream<TaxiFare> fares = env.addSource(fareSourceOrTest(new TaxiFareSource(input, maxEventDelay, servingSpeedFactor)));

		DataStream<Tuple3<Long, Long, Float>> hourlyMax = fares
				.keyBy((TaxiFare fare) -> fare.driverId)
				.timeWindow(Time.hours(1))
				.process(new SumTips())
				.timeWindowAll(Time.hours(1))
				.maxBy(2);

		printOrTest(hourlyMax);
		env.execute("Hourly Tips (java)");
	}

	public static class SumTips extends ProcessWindowFunction<TaxiFare, Tuple3<Long, Long, Float>, Long, TimeWindow> {
		@Override
		public void process(Long key, Context context, Iterable<TaxiFare> fares, Collector<Tuple3<Long, Long, Float>> out)
				throws Exception {
			Float sum = 0F;
			for (TaxiFare f : fares) {
				sum += f.tip;
			}
			out.collect(new Tuple3<>(context.window().getEnd(), key, sum));
		}
	}
}
