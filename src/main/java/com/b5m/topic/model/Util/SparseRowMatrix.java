package com.b5m.topic.model.Util;

import org.apache.mahout.math.Matrix;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.apache.mahout.math.function.Functions;

public class SparseRowMatrix extends org.apache.mahout.math.SparseRowMatrix {
	public SparseRowMatrix(int rows, int columns) {
		super(rows, columns);
	}

	@Override
	public Matrix like(int rows, int columns) {
		return new SparseRowMatrix(rows, columns);
	}

	@Override
	public Matrix like() {
		return new SparseRowMatrix(rowSize(), columnSize());
	}

	@Override
	public Matrix plus(Matrix other) {
		Matrix result = super.clone();
		for (int row = 0; row < super.rows; row++) {
			result.viewRow(row).assign(other.viewRow(row), Functions.PLUS);
		}
		return result;
	}

	@Override
	public Matrix assign(Matrix other, DoubleDoubleFunction function) {
		for (int row = 0; row < super.rows; row++) {
//			super.viewRow(row).assign(other.viewRow(row), function);
			Vector srow = super.viewRow(row);
			Vector orow = other.viewRow(row);
			for (Element e : orow.nonZeroes()) {
				int index = e.index();
				double weight = e.get();
				weight = function.apply(srow.getQuick(index), weight);
				srow.setQuick(index, weight);
			}
		}
		return this;
	}
}
