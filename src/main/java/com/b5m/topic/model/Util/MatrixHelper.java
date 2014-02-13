package com.b5m.topic.model.Util;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.iterator.sequencefile.PathType;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileDirIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterator;
import org.apache.mahout.math.DenseMatrix;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;

import com.google.common.collect.Lists;

public class MatrixHelper {
	public static Matrix matrixFromSequenceFile(Configuration conf, Path file) {
		int numTopics = -1;
		int maxItem = -1;
		List<Pair<Integer, Vector>> rows = Lists.newArrayList();
		for (Pair<IntWritable, VectorWritable> row : new SequenceFileDirIterable<IntWritable, VectorWritable>(
				file, PathType.GLOB, null, null, true, conf)) {
			rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
			numTopics = Math.max(numTopics, row.getFirst().get());
			if (maxItem < 0) {
				maxItem = row.getSecond().get().size();
			}
		}
		if (numTopics == -1) {
			return null;
		}
		numTopics++;
		// maxItem++;
		Matrix model = new DenseMatrix(numTopics, maxItem);
		for (Pair<Integer, Vector> pair : rows) {
			model.viewRow(pair.getFirst()).assign(pair.getSecond());
		}
		return model;
	}

	public static Matrix matrixFromSequenceFile(Configuration conf, Path file,
			int numCols) {
		int numTopics = -1;
		List<Pair<Integer, Vector>> rows = Lists.newArrayList();
		for (Pair<IntWritable, VectorWritable> row : new SequenceFileDirIterable<IntWritable, VectorWritable>(
				file, PathType.GLOB, null, null, true, conf)) {
			rows.add(Pair.of(row.getFirst().get(), row.getSecond().get()));
			numTopics = Math.max(numTopics, row.getFirst().get());

		}
		if (numTopics == -1) {
			return null;
		}
		numTopics++;
		Matrix model = new DenseMatrix(numTopics, numCols);
		for (Pair<Integer, Vector> pair : rows) {
			Vector row = model.viewRow(pair.getFirst());
			for (Element e : pair.getSecond().nonZeroes()) {
				row.set(e.index(), e.get());
			}
		}
		return model;
	}

	public static double similarity(Matrix lv, Matrix rv) {
		double sim = 0.0;
		if (lv.numRows() == rv.numRows()) {
			int numRows = lv.numRows();
			for (int i = 0; i < numRows; i++) {
				Vector lvector = lv.viewRow(i);
				Vector rvector = rv.viewRow(i);
				double dividend = lvector.dot(rvector);
				double divisor = lvector.getLengthSquared()
						* rvector.getLengthSquared();
				sim += dividend * dividend / divisor;
			}
			sim /= numRows;
		}
		return sim;
	}

	public static int fuzzyIndex(Matrix m, Vector v, double threshold) {
		int ret = -1;
		Vector simVector = new DenseVector(m.numRows());
		for (int i = 0; i < m.numRows(); i++) {
			Vector row = m.viewRow(i);
			double dividend = row.dot(v);
			double divisor = row.getLengthSquared() * v.getLengthSquared();
			simVector.set(i, dividend * dividend / divisor);
		}
		if (threshold < simVector.maxValue()) {
			ret = simVector.maxValueIndex();
		}
		return ret;
	}
}
