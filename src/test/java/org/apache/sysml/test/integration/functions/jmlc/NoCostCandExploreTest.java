package org.apache.sysml.test.integration.functions.jmlc;

import org.apache.sysml.api.DMLException;
import org.apache.sysml.api.DMLScript;
import org.apache.sysml.api.jmlc.Connection;
import org.apache.sysml.api.jmlc.PreparedScript;
import org.apache.sysml.conf.CompilerConfig.ConfigType;
import org.apache.sysml.runtime.matrix.data.MatrixBlock;
import org.apache.sysml.runtime.util.DataConverter;
import org.apache.sysml.test.integration.AutomatedTestBase;
import org.apache.sysml.utils.Explain;
import org.apache.sysml.utils.Statistics;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

/**
 * Candidate Exploration test for unknown sizes.
 * Validity constraints and cost comparisons require size information,
 * for now test a new candidate exploration mode for Unary operations.
 */

public class NoCostCandExploreTest extends AutomatedTestBase {
	private final static String TEST_NAME = "NoCostCandExploreTest";
	private final static String TEST_DIR  = "functions/jmlc";

	@Override
	public void setUp() {
		addTestConfiguration(TEST_DIR, TEST_NAME);
		getAndLoadTestConfiguration(TEST_NAME);
	}

	@Test
	public void TestUnaryOp() throws IOException, DMLException {

		DMLScript.STATISTICS = true;
		DMLScript.ENABLE_DEBUG_MODE = true;

		double[][] X = getRandomMatrix(10000, 1600, -10, 10, 0.001, 76543);
		MatrixBlock mX = DataConverter.convertToMatrixBlock(X);

		Connection conn = new Connection(ConfigType.CODEGEN_ENABLED, ConfigType.ALLOW_DYN_RECOMPILATION);
		String str = conn.readScript(baseDirectory + File.separator + "cand-explore.dml");
		PreparedScript script = conn.prepareScript(str, new String[] { "inScalar1", "X" }, new String[] {},
				false);

		double inScalar1 = 1;

		script.setMatrix("X", mX, false);
		script.setScalar("inScalar1", inScalar1);
		setExpectedStdOut("0");
		script.executeScript();
		conn.close();
		System.out.println(Statistics.display());
	//	System.out.println(Explain.display());
	}

}