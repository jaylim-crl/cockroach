// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Code generated by generate-logictest, DO NOT EDIT.

package test5node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/logictest"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
)

const configIdx = 7

var logicTestDir string

func init() {
	if bazel.BuiltWithBazel() {
		var err error
		logicTestDir, err = bazel.Runfile("pkg/sql/logictest/testdata/logic_test")
		if err != nil {
			panic(err)
		}
	} else {
		logicTestDir = "../../../../sql/logictest/testdata/logic_test"
	}
}

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)

	defer serverutils.TestingSetDefaultTenantSelectionOverride(
		base.TestIsForStuffThatShouldWorkWithSecondaryTenantsButDoesntYet(76378),
	)()

	os.Exit(m.Run())
}

func runLogicTest(t *testing.T, file string) {
	skip.UnderDeadlock(t, "times out and/or hangs")
	logictest.RunLogicTest(t, logictest.TestServerArgs{}, configIdx, filepath.Join(logicTestDir, file))
}

// TestLogic_tmp runs any tests that are prefixed with "_", in which a dedicated
// test is not generated for. This allows developers to create and run temporary
// test files that are not checked into the repository, without repeatedly
// regenerating and reverting changes to this file, generated_test.go.
//
// TODO(mgartner): Add file filtering so that individual files can be run,
// instead of all files with the "_" prefix.
func TestLogic_tmp(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var glob string
	glob = filepath.Join(logicTestDir, "_*")
	logictest.RunLogicTests(t, logictest.TestServerArgs{}, configIdx, glob)
}

func TestLogic_contention_event(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "contention_event")
}

func TestLogic_dist_vectorize(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "dist_vectorize")
}

func TestLogic_distsql_agg(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_agg")
}

func TestLogic_distsql_builtin(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_builtin")
}

func TestLogic_distsql_crdb_internal(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_crdb_internal")
}

func TestLogic_distsql_datetime(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_datetime")
}

func TestLogic_distsql_distinct_on(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_distinct_on")
}

func TestLogic_distsql_enum(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_enum")
}

func TestLogic_distsql_numtables(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_numtables")
}

func TestLogic_distsql_stats(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_stats")
}

func TestLogic_distsql_subquery(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_subquery")
}

func TestLogic_distsql_union(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "distsql_union")
}

func TestLogic_experimental_distsql_planning_5node(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "experimental_distsql_planning_5node")
}

func TestLogic_hash_join_dist(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "hash_join_dist")
}

func TestLogic_inv_stats(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "inv_stats")
}

func TestLogic_inverted_filter_geospatial_dist(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "inverted_filter_geospatial_dist")
}

func TestLogic_inverted_join_geospatial_dist(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "inverted_join_geospatial_dist")
}

func TestLogic_merge_join_dist(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "merge_join_dist")
}

func TestLogic_ranges(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "ranges")
}

func TestLogic_tenant_slow_repro(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	runLogicTest(t, "tenant_slow_repro")
}
