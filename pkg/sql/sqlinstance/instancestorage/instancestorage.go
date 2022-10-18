// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package instancestorage package provides API to read from and write to the
// sql_instances system table.
package instancestorage

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/multitenant"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(jaylim-crl): Consider making these values cluster settings, if necessary.
const (
	// allocateAndCleanupFrequency refers to the frequency in which the allocate
	// and cleanup background task for instance rows will run.
	allocateAndCleanupFrequency = 30 * time.Second

	// preallocatedCount refers to the number of available instance rows that
	// we want to preallocate.
	preallocatedCount = 10
)

// Storage implements the storage layer for the sqlinstance subsystem.
type Storage struct {
	codec    keys.SQLCodec
	db       *kv.DB
	tableID  descpb.ID
	slReader sqlliveness.Reader
	rowcodec rowCodec
}

// instancerow encapsulates data for a single row within the sql_instances table.
type instancerow struct {
	instanceID base.SQLInstanceID
	addr       string
	sessionID  sqlliveness.SessionID
	locality   roachpb.Locality
	timestamp  hlc.Timestamp
}

// isAvailable returns true if the instance row hasn't been claimed by a SQL pod
// (i.e. available for claiming), or false otherwise.
func (r *instancerow) isAvailable() bool {
	return r.addr == ""
}

// NewTestingStorage constructs a new storage with control for the database
// in which the sql_instances table should exist.
func NewTestingStorage(
	db *kv.DB, codec keys.SQLCodec, sqlInstancesTableID descpb.ID, slReader sqlliveness.Reader,
) *Storage {
	s := &Storage{
		db:       db,
		codec:    codec,
		tableID:  sqlInstancesTableID,
		rowcodec: makeRowCodec(codec),
		slReader: slReader,
	}
	return s
}

// NewStorage creates a new storage struct.
func NewStorage(db *kv.DB, codec keys.SQLCodec, slReader sqlliveness.Reader) *Storage {
	return NewTestingStorage(db, codec, keys.SQLInstancesTableID, slReader)
}

// CreateInstance claims a unique instance identifier for the SQL pod, and
// associates it with its SQL address and session information.
func (s *Storage) CreateInstance(
	ctx context.Context,
	sessionID sqlliveness.SessionID,
	sessionExpiration hlc.Timestamp,
	addr string,
	locality roachpb.Locality,
) (instanceID base.SQLInstanceID, _ error) {
	if len(addr) == 0 {
		return base.SQLInstanceID(0), errors.New("no address information for instance")
	}
	if len(sessionID) == 0 {
		return base.SQLInstanceID(0), errors.New("no session information for instance")
	}
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	err := s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}

		// TODO(jaylim-crl): One optimization approach is to trigger the async
		// generation loop if the number of preallocated instance rows drops to
		// less than half of the desired value. This scenario happens when we
		// start creating a lot of SQL pods before the next allocate tick. That
		// said, note that each SQL pod has its own allocate background task
		// that runs whenever the SQL pod starts up, so the described scenario
		// is extremely rare.
		instanceIDs, err := s.getAvailableInstanceIDsForRegion(ctx, txn, 1 /* count */)
		if err != nil {
			return err
		}
		if len(instanceIDs) != 1 {
			return errors.AssertionFailedf("len(instanceIDs) must be 1")
		}
		// Regardless of whether the ID is pre-allocated or not, we will write
		// to it.
		for id := range instanceIDs {
			instanceID = id
			break
		}
		row, err := s.rowcodec.encodeRow(instanceID, addr, sessionID, locality, s.codec, s.tableID)
		if err != nil {
			log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
			return err
		}
		b := txn.NewBatch()
		b.Put(row.Key, row.Value)
		return txn.CommitInBatch(ctx, b)
	})
	if err != nil {
		return base.SQLInstanceID(0), err
	}
	return instanceID, nil
}

// getAvailableInstanceIDsForRegion retrieves a list of instance IDs that are
// available for a given region. The instance IDs retrieved may or may not been
// pre-allocated. When this returns, it is guaranteed that the size of the map
// will be the same as count.
//
// TODO(jaylim-crl): Store current region enum in s?
//
// TODO(jaylim-crl): When tenant is created, the first SQL pod will be slow
// (e.g. no fields would result in one regional kv read, one global kv read, one
// regional kv write). Today. it's one global read, and one global kv write.
// Maybe there are ways to address this.
func (s *Storage) getAvailableInstanceIDsForRegion(
	ctx context.Context, txn *kv.Txn, count int,
) (map[base.SQLInstanceID]bool, error) {
	instanceIDs := make(map[base.SQLInstanceID]bool)

	// Read regional rows first.
	rows, err := s.getRegionalInstanceRows(ctx, txn)
	if err != nil {
		return nil, err
	}

	// Use pre-generated regional rows if they are available.
	for i := 0; i < len(rows) && len(instanceIDs) < count; i++ {
		if rows[i].isAvailable() {
			instanceIDs[rows[i].instanceID] = true
		}
	}

	// We are done. All the requested count are already pre-allocated.
	if len(instanceIDs) == count {
		return instanceIDs, nil
	}

	// Not enough pre-allocated regional rows, so we need to retrieve global
	// rows to obtain a new instance IDs.
	rows, err = s.getGlobalInstanceRows(ctx, txn)
	if err != nil {
		return nil, err
	}

	// No global rows, so we can be sure that the next ID must be 1 onwards.
	// If we hit this case, len(instanceIDs) must be 0 (i.e. there shouldn't be
	// any regional rows as well).
	if len(rows) == 0 {
		for i := 0; i < count; i++ {
			instanceIDs[base.SQLInstanceID(i+1)] = false
		}
		return instanceIDs, nil
	}

	// Sort instance rows in increasing order of their instance IDs so that
	// active instance IDs are as close to a contiguous sequence. There will
	// definitely be gaps since instance IDs are only chosen from their regional
	// scopes.
	//
	// For example, in the case below, 2 and 3 are unused. When allocating IDs
	// for a third region, we pick 5.
	//   1: us-east1 [USED]
	//   2: us-east1
	//   3: europe-west1
	//   4: europe-west1 [USED]
	sort.SliceStable(rows, func(idx1, idx2 int) bool {
		return rows[idx1].instanceID < rows[idx2].instanceID
	})

	// Initialize prevInstanceID with starter value of 0 as instanceIDs begin
	// from 1.
	prevInstanceID := base.SQLInstanceID(0)
	for i := 0; i < len(rows) && len(instanceIDs) < count; i++ {
		// Check for a gap between adjacent instance IDs indicating the
		// availability of an unused instance ID.
		if rows[i].instanceID-prevInstanceID > 1 {
			instanceIDs[prevInstanceID+1] = false
			prevInstanceID = prevInstanceID + 1
		} else {
			prevInstanceID = rows[i].instanceID
		}
	}

	remainingCount := count - len(instanceIDs)
	for i := 1; i <= remainingCount; i++ {
		instanceIDs[rows[len(rows)-1].instanceID+base.SQLInstanceID(i)] = false
	}
	return instanceIDs, nil
}

// getInstanceData retrieves the network address for an instance given its
// instance ID.
//
// TODO(jaylim-crl): This is only used in tests, and could potentially be removed.
func (s *Storage) getInstanceData(
	ctx context.Context, instanceID base.SQLInstanceID,
) (instanceData instancerow, _ error) {
	k := makeInstanceKey(s.codec, s.tableID, instanceID)
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	row, err := s.db.Get(ctx, k)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "could not fetch instance %d", instanceID)
	}
	if row.Value == nil {
		return instancerow{}, sqlinstance.NonExistentInstanceError
	}
	_, addr, sessionID, locality, timestamp, _, err := s.rowcodec.decodeRow(row)
	if err != nil {
		return instancerow{}, errors.Wrapf(err, "could not decode data for instance %d", instanceID)
	}
	instanceData = instancerow{
		instanceID: instanceID,
		addr:       addr,
		sessionID:  sessionID,
		timestamp:  timestamp,
		locality:   locality,
	}
	return instanceData, nil
}

// getAllInstancesData retrieves instance information on all instances for
// the tenant.
//
// TODO(jaylim-crl): This is only used in tests, and could potentially be removed.
func (s *Storage) getAllInstancesData(ctx context.Context) (instances []instancerow, err error) {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	err = s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		instances, err = s.getGlobalInstanceRows(ctx, txn)
		return err
	})
	if err != nil {
		return nil, err
	}
	return instances, nil
}

// getGlobalInstanceRows decodes and returns all instance rows across all
// regions from the sql_instances table.
//
// TODO(jaylim-crl): For now, global and regional are the same.
func (s *Storage) getGlobalInstanceRows(
	ctx context.Context, txn *kv.Txn,
) (instances []instancerow, _ error) {
	return s.getRegionalInstanceRows(ctx, txn)
}

// getRegionalInstanceRows decodes and returns all instance rows associated
// with a given region from the sql_instances table. This returns both used and
// available instance rows.
//
// TODO(rima): Add locking mechanism to prevent thrashing at startup in the
// case where multiple instances attempt to initialize their instance IDs
// simultaneously.
//
// TODO(jaylim-crl): This currently fetch all rows. We want to only fetch rows
// associated with the region of the SQL pod. This method will likely need to
// take in a region (or if we stored the region in s).
func (s *Storage) getRegionalInstanceRows(
	ctx context.Context, txn *kv.Txn,
) (instances []instancerow, _ error) {
	start := makeTablePrefix(s.codec, s.tableID)
	end := start.PrefixEnd()
	// Fetch all rows. The expected data size is small, so it should
	// be okay to fetch all rows together.
	const maxRows = 0
	rows, err := txn.Scan(ctx, start, end, maxRows)
	if err != nil {
		return nil, err
	}
	for i := range rows {
		instanceID, addr, sessionID, locality, timestamp, _, err := s.rowcodec.decodeRow(rows[i])
		if err != nil {
			log.Warningf(ctx, "failed to decode row %v: %v", rows[i].Key, err)
			return nil, err
		}
		curInstance := instancerow{
			instanceID: instanceID,
			addr:       addr,
			sessionID:  sessionID,
			timestamp:  timestamp,
			locality:   locality,
		}
		instances = append(instances, curInstance)
	}
	return instances, nil
}

// ReleaseInstanceID releases an instance ID prior to shutdown of a SQL pod
// The instance ID can then be reused by another SQL pod of the same tenant.
func (s *Storage) ReleaseInstanceID(ctx context.Context, id base.SQLInstanceID) error {
	key := makeInstanceKey(s.codec, s.tableID, id)
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	if _, err := s.db.Del(ctx, key); err != nil {
		return errors.Wrapf(err, "could not delete instance %d", id)
	}
	return nil
}

// RunAsyncAllocateAndCleanup runs a background task that allocates available
// instance IDs within the sql_instances table, and cleans used instance IDs up
// if the sessions are no longer alive.
func (s *Storage) RunAsyncAllocateAndCleanup(
	ctx context.Context, stopper *stop.Stopper, ts timeutil.TimeSource, session sqlliveness.Session,
) error {
	return stopper.RunAsyncTask(ctx, "allocate-and-cleanup", func(ctx context.Context) {
		ctx, cancel := stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		timer := ts.NewTimer()
		defer timer.Stop()
		for {
			timer.Reset(allocateAndCleanupFrequency)
			select {
			case <-ctx.Done():
				return
			case <-timer.Ch():
				timer.MarkRead()
				if err := s.generateAvailableInstanceRows(ctx, session.Expiration()); err != nil {
					log.Warningf(ctx, "failed to generate available instance rows: %v", err)
				}
				// TODO(jaylim-crl): If this step blocks for a long time, we may
				// end up blocking the entire loop, causing instance rows to not
				// be generated. We can potentially solve this by splitting the
				// cleanup into a different background task.
				if err := s.cleanupExpiredInstanceRows(ctx, session.Expiration()); err != nil {
					log.Warningf(ctx, "failed to cleanup expired instance rows: %v", err)
				}
			}
		}
	})
}

// generateAvailableInstanceRows allocates available instance IDs, and store
// them in the sql_instances table. When instance IDs are pre-allocated, all
// other fields in that row will be NULL.
//
// TODO(jaylim-crl): Handle multiple regions in this logic. encodeRow has to be
// updated with crdb_region. When we handle multiple regions, we have to figure
// out where to get the list of regions, and ensure that we don't do a global
// read for each region assuming that the number of pre-allocated entries is
// insufficient (i.e. we may not be able to use getAvailableInstanceIDsForRegion).
// One global KV read and write would be sufficient for all regions.
func (s *Storage) generateAvailableInstanceRows(
	ctx context.Context, sessionExpiration hlc.Timestamp,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}

		instanceIDs, err := s.getAvailableInstanceIDsForRegion(ctx, txn, preallocatedCount)
		if err != nil {
			return err
		}

		b := txn.NewBatch()
		for instanceID, preallocated := range instanceIDs {
			// Nothing to do if ids have already been pre-allocated.
			if preallocated {
				continue
			}
			row, err := s.rowcodec.encodeRow(
				instanceID, "", sqlliveness.SessionID([]byte{}), roachpb.Locality{}, s.codec, s.tableID,
			)
			if err != nil {
				log.Warningf(ctx, "failed to encode row for instance id %d: %v", instanceID, err)
				return err
			}
			b.Put(row.Key, row.Value)
		}
		return txn.CommitInBatch(ctx, b)
	})
}

// cleanupExpiredInstanceRows cleans up instance rows with expired sessions.
// Note that the creation scheme does not attempt to reuse a row. We would have
// to delete the row first, and then pre-allocate a new one.
func (s *Storage) cleanupExpiredInstanceRows(
	ctx context.Context, sessionExpiration hlc.Timestamp,
) error {
	ctx = multitenant.WithTenantCostControlExemption(ctx)
	return s.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Set the transaction deadline to the session expiration to ensure
		// transaction commits before the session expires.
		err := txn.UpdateDeadline(ctx, sessionExpiration)
		if err != nil {
			return err
		}

		// If we only clean up regional entries here, then we may end up in a
		// situation where regions without SQL pods won't have their entries
		// cleaned up.
		rows, err := s.getGlobalInstanceRows(ctx, txn)
		if err != nil {
			return err
		}

		// Instance IDs can be associated with a dead session if the ID cleanup
		// phase does not occur during SQL pod shutdown (e.g. instance panic).
		var keys []interface{}
		for _, row := range rows {
			// Don't clean up available rows since they are unused/preallocated.
			if row.isAvailable() {
				continue
			}
			sessionAlive, _ := s.slReader.IsAlive(ctx, row.sessionID)
			if !sessionAlive {
				keys = append(keys, makeInstanceKey(s.codec, s.tableID, row.instanceID))
			}
		}

		b := txn.NewBatch()
		b.Del(keys...)
		return txn.CommitInBatch(ctx, b)
	})
}
