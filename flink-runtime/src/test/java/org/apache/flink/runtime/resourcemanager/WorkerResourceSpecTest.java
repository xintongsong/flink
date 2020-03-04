/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.resourcemanager;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for {@link WorkerResourceSpec}.
 */
public class WorkerResourceSpecTest {

	@Test
	public void testEquals() {
		final WorkerResourceSpec spec1 = new WorkerResourceSpec(1.0, 100, 100, 100, 100);
		final WorkerResourceSpec spec2 = new WorkerResourceSpec(1.0, 100, 100, 100, 100);
		final WorkerResourceSpec spec3 = new WorkerResourceSpec(1.1, 100, 100, 100, 100);
		final WorkerResourceSpec spec4 = new WorkerResourceSpec(1.0, 110, 100, 100, 100);
		final WorkerResourceSpec spec5 = new WorkerResourceSpec(1.0, 100, 110, 100, 100);
		final WorkerResourceSpec spec6 = new WorkerResourceSpec(1.0, 100, 100, 110, 100);
		final WorkerResourceSpec spec7 = new WorkerResourceSpec(1.0, 100, 100, 100, 110);

		assertEquals(spec1, spec1);
		assertEquals(spec1, spec2);
		assertNotEquals(spec1, spec3);
		assertNotEquals(spec1, spec4);
		assertNotEquals(spec1, spec5);
		assertNotEquals(spec1, spec6);
		assertNotEquals(spec1, spec7);
	}

	@Test
	public void testHashCodeEquals() {
		final WorkerResourceSpec spec1 = new WorkerResourceSpec(1.0, 100, 100, 100, 100);
		final WorkerResourceSpec spec2 = new WorkerResourceSpec(1.0, 100, 100, 100, 100);
		final WorkerResourceSpec spec3 = new WorkerResourceSpec(1.1, 100, 100, 100, 100);
		final WorkerResourceSpec spec4 = new WorkerResourceSpec(1.0, 110, 100, 100, 100);
		final WorkerResourceSpec spec5 = new WorkerResourceSpec(1.0, 100, 110, 100, 100);
		final WorkerResourceSpec spec6 = new WorkerResourceSpec(1.0, 100, 100, 110, 100);
		final WorkerResourceSpec spec7 = new WorkerResourceSpec(1.0, 100, 100, 100, 110);

		assertEquals(spec1.hashCode(), spec1.hashCode());
		assertEquals(spec1.hashCode(), spec2.hashCode());
		assertNotEquals(spec1.hashCode(), spec3.hashCode());
		assertNotEquals(spec1.hashCode(), spec4.hashCode());
		assertNotEquals(spec1.hashCode(), spec5.hashCode());
		assertNotEquals(spec1.hashCode(), spec6.hashCode());
		assertNotEquals(spec1.hashCode(), spec7.hashCode());
	}
}
