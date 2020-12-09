package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/mitchellh/go-homedir"
	"io/ioutil"
	"log"
	"path/filepath"
	"sync"
	cid "github.com/ipfs/go-cid"
)

type ActorID uint64

type SectorNumber uint64

type SectorWorkingPhase int

type TaskType string

type Randomness []byte

type PaddedPieceSize uint64

type PreCommit1Out []byte

type Proof []byte

type FileTaskType string

type Commit1Out []byte

type SectorID struct {
	Miner  ActorID
	Number SectorNumber
}
type PieceInfo struct {
	Size     PaddedPieceSize // Size in nodes. For BLS12-381 (capacity 254 bits), must be >= 16. (16 * 8 = 128)
	PieceCID cid.Cid
}

type SectorCids struct {
	Unsealed cid.Cid
	Sealed   cid.Cid
}

type RegisteredSealProof int64
type SealRandomness Randomness
type InteractiveSealRandomness Randomness
type TaskInfo struct {
	SectorID         SectorID
	TaskType         TaskType
	SealProofType    RegisteredSealProof
	CacheDirPath     string
	StagedSectorPath string
	SealedSectorPath string
	Ticket           SealRandomness
	Seed             InteractiveSealRandomness
	Pieces           []PieceInfo
	PreCommit1Out    PreCommit1Out
	PreCommit2Out    SectorCids
	Commit1Out       Commit1Out
	Commit2Out       Proof
	Finalized        bool
	ErrMsg           string
}

type FileTask struct {
	ID                       uuid.UUID
	SectorID                 SectorID
	FileTaskType             FileTaskType
	SealProofType            RegisteredSealProof
	SourceUnsealedSectorPath string
	SourceSealedSectorPath   string
	SourceCachePath          string
	TargetUnsealedSectorPath string
	TargetSealedSectorPath   string
	TargetCachePath          string
	ErrMsg                   string
	Done                     bool
}

type SectorRecord struct {
	SectorId                SectorID
	SectorWorkingPhase      SectorWorkingPhase
	CurrentSealTask         TaskInfo
	CurrentFileTask         FileTask
	MinerUnsealedSectorPath string
	MinerSealedSectorPath   string
	MinerCacheDirPath       string

	P1WorkerAddress      string
	P1UnsealedSectorPath string
	P1SealedSectorPath   string
	P1CacheDirPath       string

	P2WorkerAddress string
	// NVMEs
	P2SealedSectorPath string
	P2CacheDirPath     string

	C1WorkerAddress    string
	C1SealedSectorPath string
	C1CacheDirPath     string

	C2WorkerAddress string
}

func loadByGob(data interface{}, filename string) error {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(raw)
	dec := gob.NewDecoder(buffer)
	err = dec.Decode(data)
	if err != nil {
		return err
	}
	return nil
}

func loadByJson(filename string) ([]SectorRecord, error) {
	raw, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	recordList := make([]SectorRecord, 0)
	err = json.Unmarshal(raw, &recordList)
	if err != nil {
		return nil, err
	}
	return recordList, nil
}

type State struct {
	filePath string
	state    map[SectorID]SectorRecord
}

var onceState sync.Once
var stateSingleton *State

func loadStateFromFile(filePath string) *State {
	onceState.Do(func() {
		stateSingleton = &State{
			filePath: filePath,
			state:    make(map[SectorID]SectorRecord),
		}
		recordList, err := loadByJson(filePath)
		if err != nil {
			fmt.Println(err.Error())
			err = loadByGob(&stateSingleton.state, filePath)
			if err != nil {
				panic(err)
			}
		} else {
			for _, v := range recordList {
				stateSingleton.state[v.SectorId] = v
			}
		}
	})
	return stateSingleton
}

func storeByJson(data map[SectorID]SectorRecord, filename string) error {
	var recordList = make([]SectorRecord, 0)
	for _, v := range data {
		recordList = append(recordList, v)
	}
	marshaled, err := json.Marshal(recordList)
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(filename, marshaled, 0600)
	if err != nil {
		return err
	}
	return nil
}

func getAbsPath(p string) (string, error) {
	newPath, err := homedir.Expand(p)
	if err != nil {
		return "", err
	}
	newPath, err = filepath.Abs(newPath)
	if err != nil {
		return "", err
	}
	return newPath, nil
}

func getState() *State {
	if stateSingleton == nil {
		panic("get stateSingleton before initialize.")
	}
	return stateSingleton
}

func (s *State) save() error {
	s.cleanCommit1Out()
	err := storeByJson(s.state, s.filePath)
	if err != nil {
		return err
	}
	return nil
}

const (
	TTCommit2 TaskType = "seal/v0/commit/2"
)

func (s *State) cleanCommit1Out() {
	for id := range s.state {
		r := s.state[id]
		if r.CurrentSealTask.TaskType == TTCommit2 {
			r.CurrentSealTask.Commit1Out = make([]byte, 0)
			s.updateSectorRecord(r)
		}
	}
}

func (s *State) updateSectorRecord(r SectorRecord) error {
	sr, ok := s.state[r.SectorId]
	if !ok {
		return errors.New("sector Id not found")
	}
	if sr.SectorWorkingPhase != r.SectorWorkingPhase {
		log.Fatalf("%v SectorWorkingPhase from %d to %d", r.SectorId, sr.SectorWorkingPhase, r.SectorWorkingPhase)
	}
	s.state[r.SectorId] = r
	return nil
}

func main() {
	path, err := getAbsPath("~/.lotus_scheduler/state_data")
	if err != nil {
		log.Fatal(err)
	}
	loadStateFromFile(path)
	err = getState().save()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println("done ok")
	}
}
