/*
 * Copyright 2020 by Beat Hoermann
 * Public ECDSA key: 04024D84F56C051C1CD0A27A54F7B73664D20CAD278D84F49261C92A
 * B06B13F104B4109BA9515AC68A8CE73A3720078BB52C22F84092C6DDE2C149EC2F5CC5A62F
 * This code is licensed under "Hoermann License"
 */
package acdp.internal.misc.array;

/**
 * This class computes the node sizes of a dynamically growing byte array
 * implemented as a sequence of nodes where each node points to a sub array of
 * the array, such that the first node points to the sub array storing the
 * first t<sub>1</sub> bytes, the second node points to the sub array storing
 * the next t<sub>2</sub> bytes and so on (0 &lt; t<sub>i</sub> for 1 &le; i,
 * t<sub>i</sub> being the node size of the i-th node, i.e., the size of the
 * i-th sub array).
 * <p>
 * The implementation of this class heavily draws on the unpublished paper
 * "Speicherung von Byte-Folgen in einer Liste".
 * If you need to understand the details of the implementation then read the
 * article which you can get from the author of this class.
 * <p>
 * Subsequent calls to the {@link #getNextSize()} method return the node sizes
 * t<sub>1</sub>, t<sub>2</sub> etc.
 *
 * @author Beat Hoermann
 */
class NodeSizer {
	private static final int MIN_NODE_SIZE = 4;
	private static final double LN2 = Math.log(2.0);
	private static final double LNZ5 = Math.log(0.5);
	
	private final double alpha;
	private final int m;

	private double t;
	private int i = 0;
	
	/**
	 * The standard constructor.
	 * Just calls the {@link #NodeSizer(int, long, double)} constructor with the
	 * first two parameters set to zero and the third set to 17.0.
	 */
	NodeSizer() {
		this(0, 0, 17.0);
	}
	
	/**
	 * Computes the growth restriction {@code m}.
	 * 
	 * @param  alpha1 The result of <code>1.0-&alpha;</code>.
	 * @param  lnAlpha The result of <code>ln(&alpha;)</code>.
	 * @param  lnAlphaNu The result of
	 * 										<code>2*ln(&alpha;)*ln(&alpha;)/&nu;</code>.
	 * @param  t The initial node size.
	 * @param  c The capacity.
	 * @param  nu The node parameter <code>&nu;</code>.
	 * @return The growth restriction {@code m}.
	 */
	private int computeM(double alpha1, double lnAlpha, double lnAlphaNu,
																double t, long c, double nu) {
		double sqrt = Math.sqrt(1.0 + lnAlphaNu *(c - t / alpha1));
		double expr = alpha1 < 0 ? -1.0 + sqrt : -1.0 - sqrt;
		expr = nu / (lnAlpha * t) * expr;
		return (int) Math.round(1.0 + Math.log(expr) / lnAlpha);
	}
	
	/**
	 * Computes the growth restriction {@code m} for <code>&alpha;</code> equal
	 * to 2.
	 * 
	 * @param  t The initial node size.
	 * @param  c The capacity.
	 * @param  nu The node parameter <code>&nu;</code>.
	 * @param  nu2 The result of <code>2/&nu;</code>.
	 * @return The growth restriction {@code m} for <code>&alpha;</code> equal
	 *         to 2.
	 */
	private int computeM2(double t, long c, double nu, double nu2) {
		return computeM(-1.0, LN2, nu2 * LN2 * LN2, t, c, nu);
	}
	
	/**
	 * Computes the value t<sup><code>&alpha;</code> &rarr; 1</sup>.
	 * 
	 * @param  c The capacity.
	 * @param  nu The node parameter <code>&nu;</code>.
	 * @param  nu2 The result of <code>2/&nu;</code>.
	 * @return The value t<sup><code>&alpha;</code> &rarr; 1</sup>.
	 */
	private double computeTAlpha1(long c, double nu, double nu2) {
		return nu * (Math.sqrt(nu2 * c + 0.25) - 0.5);
	}
	
	/**
	 * Computes the improved value of the initial node size.
	 * 
	 * @param  alpha The growth parameter <code>&alpha;</code>.
	 * @param  c The capacity.
	 * @param  m The growth restriction.
	 * @param  nu The node parameter <code>&nu;</code>.
	 * @return The improved value of the initial node size.
	 */
	private double computeTStar(double alpha, long c, int m, double nu) {
		double expr = Math.sqrt(2.0 * nu * c);
		return m == 1 ? expr : expr / Math.pow(alpha, m - 1);
	}
	
	/**
	 * The semi-expert constructor.
	 * The values of the first two parameters can be specified to be less than
	 * or equal to zero.
	 * In such a case this constructor chooses reasonable values for {@code t}
	 * and {@code c}.
	 * For computing the growth parameter <code>&alpha;</code> and the growth
	 * restriction {@code m} this method uses the following values for {@code t}
	 * and {@code c}:
	 * <h1>{@code t} &le; 0, {@code c} &le; 0</h1>
	 * The initial node size {@code t} is set to 4.
	 * The capacity {@code c} is set equal to the value of {@code CAPACITY}
	 * which is the maximum of {@code MIN_CAPACITY} (which is equal to the value
	 * returned by <code>Math.ceil(2*&nu;)</code>) and the value returned by the
	 * {@code Runtime.getRuntime().maxMemory()}-method.
	 * <h1>{@code t} &gt; 0, {@code c} &le; 0</h1>
	 * The value of the initial node size {@code t} is equal to the specified
	 * value.
	 * The capacity {@code c} is set to the maximum of the value of {@code t}
	 * and {@code CAPACITY}.
	 * <h1>{@code t} &le; 0, {@code c} &gt; 0</h1>
	 * The initial node size {@code t} is set to 4.
	 * If the specified value of {@code c} is less than {@code MIN_CAPACITY}
	 * then {@code c} is set equal to the value of {@code MIN_CAPACITY}.
	 * <h1>{@code t} &gt; 0, {@code c} &gt; 0</h1>
	 * If {@code t} is greater than {@code c} then this method throws an
	 * {@link IllegalArgumentException}.
	 * Otherwise, the value of {@code t} is equal to the specified value.
	 * If the specified value of {@code c} is less than {@code MIN_CAPACITY}
	 * then {@code c} is set equal to the value of {@code MIN_CAPACITY}.
	 * <p>
	 * This method tries to improve the value of the initial node size {@code t}.
	 * <p>
	 * The value for the growth parameter <code>&alpha;</code> will always be
	 * computed equal to 2.0 and 0.5^i (1 &le; i) for a byte array with growing
	 * and shrinking node sizes, respectively.
	 * (The exact value of i depends on the values of {@code t} and {@code c}.)
	 * <p>
	 * Call the expert constructor {@link #NodeSizer(double, int, long, double)}
	 * to pass your own, unchanged parameters for {@code t}, {@code c} and even
	 * <code>&alpha;</code>.
	 * 
	 * @param  t The initial node size.
	 *         Set the value to the exact or estimated minimum number of bytes
	 *         in the array.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 * @param  c The capacity.
	 *         Set the value to the <em>estimated</em> maximum number of bytes
	 *         in the array.
	 *         If you do not know such a number then set the value to be less
	 *         than or equal to zero.
	 *         In such a case the capacity is internally set to half of the
	 *         current amount of total free memory.
	 * @param  nu The node parameter <code>&nu;</code>, hence, the exact or
	 *         estimated number of overhead in bytes of a single node.
	 *         The value depends on how a node is implemented.
	 * @throws IllegalArgumentException If the specified value for
	 *         <code>&nu;</code> is less than 4 or if the specified values for
	 *         {@code t} and {@code c} are both greater than zero and at the
	 *         same time the specified value for {@code t} is greater than the
	 *         specified value for {@code c}.
	 */
	NodeSizer(int t, long c, double nu) throws IllegalArgumentException {
		if (nu < 4) {
			throw new IllegalArgumentException();
		}

		final double nu2 = 2.0/nu;
		final long MIN_CAPACITY = (long) Math.ceil(2.0*nu);
		
		if (t <= 0 && c <= 0) {
			final Runtime rt = Runtime.getRuntime();
			c = Math.max(MIN_CAPACITY, (rt.maxMemory() - rt.totalMemory() +
																			rt.freeMemory()) / 2);
			this.t = MIN_NODE_SIZE;
			this.alpha = 2.0;
			this.m = computeM2(this.t, c, nu, nu2);
		}
		else {
			// t > 0 || c > 0
			// Set tD and this.c.
			double tD;
			if (c <= 0) {
				tD = t;
				final Runtime rt = Runtime.getRuntime();
				c = Math.max(t, Math.max(MIN_CAPACITY, (rt.maxMemory() -
												rt.totalMemory() + rt.freeMemory()) / 2));
			}
			else if (t <= 0) {
				tD = MIN_NODE_SIZE;
				c = Math.max(MIN_CAPACITY, c);
			}
			else if (t <= c) {
				tD = t;
				c = Math.max(MIN_CAPACITY, c);
			}
			else {
				// t > 0 && c > 0 && t > c
				throw new IllegalArgumentException();
			}
			// 0 < tD <= c
			
			// Set this.alpha, compute this.m and this.t.
			if (tD < nu) {
				this.alpha = 2.0;
				this.m = computeM2(tD, c, nu, nu2);
				this.t = computeTStar(this.alpha, c, this.m, nu);
			}
			else {
				double tAlpha1 = computeTAlpha1(c, nu, nu2);
				
				if (tD == tAlpha1) {
					this.alpha = 1.0;
					this.m = 1;
					this.t = computeTStar(this.alpha, c, this.m, nu);
				}
				else if (tD < tAlpha1) {
					this.alpha = 2.0;
					this.m = computeM2(tD, c, nu, nu2);
					this.t = computeTStar(this.alpha, c, this.m, nu);
				}
				else {
					// tD > tAlpha1
					double alpha = 0.5;
						
					// Compute tMax.
					double alpha1 = 0.5;
					double lnAlpha = LNZ5;
					double lnAlphaNuInv = nu / (2.0 * LNZ5 * LNZ5);
					double tMax = alpha1 * (lnAlphaNuInv + c);
				
					while (tD > tMax && alpha >= 0.05) {
						alpha /= 2.0;
							
						alpha1 = 1.0 - alpha;
						lnAlpha = Math.log(alpha);
						lnAlphaNuInv = nu / (2.0 * lnAlpha * lnAlpha);
						tMax = alpha1 * (lnAlphaNuInv + c);
					}
						
					this.alpha = alpha;
						
					if (tD > tMax) {
						tD = tMax;
					}
					// 0 < t <= this.c && t <= tMax
						
					// Compute m.
					this.m = computeM(alpha1, lnAlpha, 1/lnAlphaNuInv, tD, c, nu);
						
					// Compute tStar.
					double tStar = computeTStar(this.alpha, c, this.m, nu);
						
					// Compute final value for t.
					this.t = tStar > tMax ? tMax : tStar;
				}
			}
		}
	}
	
	/**
	 * The expert constructor.
	 * Use this constructor if you know the exact meaning of the parameters
	 * below.
	 * This method tests the specified values for feasibility.
	 * If they are feasible, this method computes the value of the growth
	 * restriction {@code m} with the specified values for <code>&alpha;</code>,
	 * {@code t}, {@code c} and <code>&nu;</code>
	 * (This method does not try to improve the specified value of {@code t}.)
	 * 
	 * @param  alpha The growth parameter <code>&alpha;</code>.
	 *         The value must be greater than zero.
	 *         Setting this value to 1 computes {@code m} to be equal to 1.
	 * @param  t The initial node size.
	 *         Set the value to the exact or estimated number of bytes the byte
	 *         array should store <em>at least</em>.
	 *         The value must be greater than zero.
	 * @param  c The capacity.
	 *         Set the value to the <em>estimated</em> number of bytes the byte
	 *         array should store <em>at most</em>.
	 *         The value must be greater than zero.
	 * @param  nu The node parameter <code>&nu;</code>, hence, the exact or
	 *         estimated number of overhead in bytes of a single node.
	 *         The value depends on how a node is implemented.
	 * @throws IllegalArgumentException If the test for feasibility of the
	 *         specified values for <code>&alpha;</code>, {@code t}, {@code c}
	 *         and <code>&nu;</code> fails.
	 */
	NodeSizer(double alpha, int t, long c, double nu)
															throws IllegalArgumentException {
		if (alpha <= 0 || t <= 0 || c <= 0 || t > c || nu < 4) {
			throw new IllegalArgumentException();
		}
		// alpha > 0 && t > 0 && c > 0 && t <= c && nu >= 4

		// Check for plausible values of alpha, t and c and compute m.
		if (alpha == 1.0)
			this.m = 1;
		else {
			double alpha1 = 1.0 - alpha;
			double lnAlpha = Math.log(alpha);
			double nu2 = 2.0 / nu;
			double lnAlphaNu = nu2*lnAlpha*lnAlpha;
			
			if (alpha < 1.0) {
				// Check for alpha <= alphaMax and t <= tMax.
				if (alpha > Math.exp(-nu/t) || t > alpha1 * (1.0 / lnAlphaNu + c)) {
					throw new IllegalArgumentException();
				}
			}
			
			double tAlpha1 = computeTAlpha1(c, nu, nu2);
			if (t == tAlpha1) 
				this.m = 1;
			else {
				// Check the appropriateness of alpha != 1.
				if ((tAlpha1 - t)*(alpha - 1.0) < 0) {
					throw new IllegalArgumentException();
				}
				this.m = computeM(alpha1, lnAlpha, lnAlphaNu, t, c, nu);
			}
		}
		
		this.alpha = alpha;
		this.t = t;
	}
	
	
	/**
	 * Returns the next node size, beginning with the initial node size
	 * t<sub>1</sub>.
	 * (The node size is the size of the byte array referenced by the node.)
	 * 
	 * @return The next node size, always greater than 0.
	 */
	final double getNextSize() {
		i++;
		if (i <= m && i > 1) {
			t *= alpha;
		}
		return t;
	}
}
