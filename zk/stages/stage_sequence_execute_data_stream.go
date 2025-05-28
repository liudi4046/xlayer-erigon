package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/zk/datastream/server"
	"github.com/ledgerwatch/erigon/zk/utils"
	"github.com/ledgerwatch/log/v3"
)

type SequencerBatchStreamWriter struct {
	batchContext *BatchContext
	batchState   *BatchState
	ctx          context.Context
	logPrefix    string
	sdb          *stageDb
	streamServer server.DataStreamServer
	hasExecutors bool
}

func newSequencerBatchStreamWriter(batchContext *BatchContext, batchState *BatchState) *SequencerBatchStreamWriter {
	return &SequencerBatchStreamWriter{
		batchContext: batchContext,
		batchState:   batchState,
		ctx:          batchContext.ctx,
		logPrefix:    batchContext.s.LogPrefix(),
		sdb:          batchContext.sdb,
		streamServer: batchContext.cfg.dataStreamServer,
		hasExecutors: batchState.hasExecutorForThisBatch,
	}
}

func (sbc *SequencerBatchStreamWriter) WriteBlockToDatastream(blockNumber uint64, batchNumber uint64, forkId uint64) error {
	// Write block directly to datastream without verification
	previousBlock, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, blockNumber-1)
	if err != nil {
		return err
	}

	block, err := rawdb.ReadBlockByNumber(sbc.sdb.tx, blockNumber)
	if err != nil {
		return err
	}

	// Get previous block's batch number
	previousBlockBatchNumber := batchNumber
	if blockNumber > 1 {
		var found bool
		previousBlockBatchNumber, found, err = sbc.sdb.hermezDb.HermezDbReader.CheckBatchNoByL2Block(previousBlock.NumberU64())
		if !found || err != nil {
			// If not found, assume same batch
			previousBlockBatchNumber = batchNumber
		}
	}

	// Write block to datastream
	if err := sbc.streamServer.WriteBlockWithBatchStartToStream(sbc.logPrefix, sbc.sdb.tx, sbc.sdb.hermezDb, forkId, batchNumber, previousBlockBatchNumber, *previousBlock, *block); err != nil {
		return err
	}

	// Update datastream progress
	if err = stages.SaveStageProgress(sbc.sdb.tx, stages.DataStream, block.NumberU64()); err != nil {
		return err
	}

	return nil
}

// writeBlockDetailsToDatastream method removed as it depends on legacy verifier

func alignExecutionToDatastream(batchContext *BatchContext, lastExecutedBlock uint64, u stagedsync.Unwinder) (bool, error) {
	lastStartedDatastreamBatch, err := batchContext.cfg.dataStreamServer.GetHighestBatchNumber()
	if err != nil {
		return false, err
	}

	lastClosedDatastreamBatch, err := batchContext.cfg.dataStreamServer.GetHighestClosedBatch()
	if err != nil {
		return false, err
	}

	lastDatastreamBlock, err := batchContext.cfg.dataStreamServer.GetHighestBlockNumber()
	if err != nil {
		return false, err
	}

	if lastStartedDatastreamBatch != lastClosedDatastreamBatch {
		if err := finalizeLastBatchInDatastreamIfNotFinalized(batchContext, lastStartedDatastreamBatch, lastDatastreamBlock); err != nil {
			return false, err
		}
	}

	if lastExecutedBlock > lastDatastreamBlock {
		block, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, lastDatastreamBlock)
		if err != nil {
			return false, err
		}

		log.Warn(fmt.Sprintf("[%s] Unwinding due to a datastream gap", batchContext.s.LogPrefix()), "streamHeight", lastDatastreamBlock, "sequencerHeight", lastExecutedBlock)
		u.UnwindTo(lastDatastreamBlock, stagedsync.BadBlock(block.Hash(), fmt.Errorf("received bad block")))
		return true, nil
	}

	if lastExecutedBlock < lastDatastreamBlock {
		panic(fmt.Errorf("[%s] Datastream is ahead of sequencer. Re-sequencing should have handled this case before even comming to this point", batchContext.s.LogPrefix()))
	}

	return false, nil
}

func finalizeLastBatchInDatastreamIfNotFinalized(batchContext *BatchContext, batchToClose, blockToCloseAt uint64) error {
	isLastEntryBatchEnd, err := batchContext.cfg.dataStreamServer.IsLastEntryBatchEnd()
	if err != nil {
		return err
	}
	if isLastEntryBatchEnd {
		return nil
	}
	log.Warn(fmt.Sprintf("[%s] Last datastream's batch %d was not closed, closing it now...", batchContext.s.LogPrefix(), batchToClose))
	return finalizeLastBatchInDatastream(batchContext, batchToClose, blockToCloseAt)
}

func finalizeLastBatchInDatastream(batchContext *BatchContext, batchToClose, blockToCloseAt uint64) error {
	ler, err := utils.GetBatchLocalExitRootFromSCStorageByBlock(blockToCloseAt, batchContext.sdb.hermezDb.HermezDbReader, batchContext.sdb.tx)
	if err != nil {
		return err
	}
	lastBlock, err := rawdb.ReadBlockByNumber(batchContext.sdb.tx, blockToCloseAt)
	if err != nil {
		return err
	}
	root := lastBlock.Root()
	if err = batchContext.cfg.dataStreamServer.WriteBatchEnd(batchContext.sdb.hermezDb, batchToClose, &root, &ler); err != nil {
		return err
	}
	return nil
}
