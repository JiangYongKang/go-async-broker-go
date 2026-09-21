package broker

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	walName    = "broker.wal"
	lockName   = "broker.lock"
	tmpSuffix  = ".tmp"
	frameMagic = uint32(0x4252_4f4b) // "BROK"
	frameData  = byte(1)
	frameBegin = byte(2)
	frameEnd   = byte(3)
)

// Store is a small durable write-ahead log. Only a complete batch terminated by
// an end frame becomes visible; a torn trailing write is truncated on open.
// All application state is therefore committed or not committed as one unit.
type Store struct {
	mu     sync.Mutex
	dir    string
	wal    *os.File
	lock   *os.File
	closed bool
}

// walFrame is stored as:
// magic(4) + kind(1) + length(4) + crc32(payload)(4) + payload.
type walFrame struct {
	kind    byte
	payload []byte
}

func OpenStore(dir string) (*Store, error) {
	if dir == "" {
		return nil, invalidf("storage directory must not be empty")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, brokerError(CodeCorruptStorage, "create storage directory", dir, err)
	}

	lock, err := acquireFileLock(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	if err := removeTemporaryFiles(dir); err != nil {
		_ = releaseFileLock(lock)
		return nil, err
	}

	path := filepath.Join(dir, walName)
	wal, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		_ = releaseFileLock(lock)
		return nil, brokerError(CodeCorruptStorage, "open write-ahead log", path, err)
	}

	if err := wal.Sync(); err != nil {
		_ = wal.Close()
		_ = releaseFileLock(lock)
		return nil, brokerError(CodeCorruptStorage, "sync write-ahead log", path, err)
	}
	if err := syncDir(dir); err != nil {
		_ = wal.Close()
		_ = releaseFileLock(lock)
		return nil, err
	}

	store := &Store{dir: dir, wal: wal, lock: lock}
	// Recovering here truncates a torn trailing batch if the previous process
	// died mid-append. O_APPEND ensures future writes go to the recovered EOF.
	if _, _, err := store.ReadEntries(); err != nil {
		_ = wal.Close()
		_ = releaseFileLock(lock)
		return nil, err
	}

	return store, nil
}

func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	var firstErr error
	if err := s.wal.Sync(); err != nil {
		firstErr = brokerError(CodeCorruptStorage, "sync write-ahead log on close", s.dir, err)
	}
	if err := s.wal.Close(); err != nil && firstErr == nil {
		firstErr = brokerError(CodeCorruptStorage, "close write-ahead log", s.dir, err)
	}
	if err := releaseFileLock(s.lock); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// Commit atomically appends one batch of JSON entries. The process can die at
// any point; recovery sees either none or every entry from the batch.
func (s *Store) Commit(entries []json.RawMessage) error {
	if len(entries) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrBrokerClosed
	}

	prepared := make([]walFrame, 0, len(entries)+2)
	prepared = append(prepared, walFrame{kind: frameBegin})
	for _, entry := range entries {
		raw := append([]byte(nil), entry...)
		if !json.Valid(raw) {
			return invalidf("WAL entry is not valid JSON")
		}
		prepared = append(prepared, walFrame{kind: frameData, payload: raw})
	}
	prepared = append(prepared, walFrame{kind: frameEnd})

	offset, err := s.wal.Seek(0, io.SeekEnd)
	if err != nil {
		return brokerError(CodeCorruptStorage, "read WAL write offset", s.dir, err)
	}
	for _, frame := range prepared {
		if err := writeFrame(s.wal, frame); err != nil {
			// Attempt to roll the file back to the batch start. A process that
			// dies here is handled by recovery: the un-terminated tail is ignored.
			_ = s.wal.Truncate(offset)
			_ = s.wal.Sync()
			return err
		}
	}
	if err := s.wal.Sync(); err != nil {
		_ = s.wal.Truncate(offset)
		_ = s.wal.Sync()
		return brokerError(CodeCorruptStorage, "fsync commit", s.dir, err)
	}
	return nil
}

func writeFrame(w io.Writer, frame walFrame) error {
	payloadLen := len(frame.payload)
	header := make([]byte, 0, 13)
	var magic [4]byte
	binary.BigEndian.PutUint32(magic[:], frameMagic)
	header = append(header, magic[:]...)
	header = append(header, frame.kind)
	length := uint32(payloadLen)
	var lengthBytes [4]byte
	binary.BigEndian.PutUint32(lengthBytes[:], length)
	header = append(header, lengthBytes[:]...)
	checksum := crc32.ChecksumIEEE(frame.payload)
	var crcBytes [4]byte
	binary.BigEndian.PutUint32(crcBytes[:], checksum)
	header = append(header, crcBytes[:]...)

	if _, err := w.Write(header); err != nil {
		return brokerError(CodeCorruptStorage, "write WAL frame header", "", err)
	}
	if payloadLen > 0 {
		if _, err := w.Write(frame.payload); err != nil {
			return brokerError(CodeCorruptStorage, "write WAL frame payload", "", err)
		}
	}
	return nil
}

// ReadEntries replays complete batches. It returns a trimmed flag when a torn
// trailing batch/frame was found. It does not ignore checksum failures, which
// indicate real corruption rather than a crash.
func (s *Store) ReadEntries() (entries []json.RawMessage, trimmed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := filepath.Join(s.dir, walName)
	f, err := os.Open(path)
	if err != nil {
		return nil, false, brokerError(CodeCorruptStorage, "open WAL for recovery", path, err)
	}
	defer f.Close()

	var validEnd int64
	var batch []json.RawMessage
	var inBatch bool

	for {
		startOffset, frame, perr := readFrame(f)
		if errors.Is(perr, io.EOF) {
			break
		}
		if perr != nil {
			if errors.Is(perr, errTornFrame) {
				if err := trimTornTail(path, validEnd); err != nil {
					return nil, false, err
				}
				return entries, true, nil
			}
			return nil, false, perr
		}

		switch frame.kind {
		case frameBegin:
			inBatch = true
			batch = batch[:0]
		case frameData:
			if !inBatch {
				return nil, false, brokerError(CodeCorruptStorage, "WAL data frame outside batch", fmt.Sprintf("offset=%d", startOffset), nil)
			}
			batch = append(batch, frame.payload)
		case frameEnd:
			if !inBatch {
				return nil, false, brokerError(CodeCorruptStorage, "WAL end frame without begin", fmt.Sprintf("offset=%d", startOffset), nil)
			}
			entries = append(entries, batch...)
			batch = nil
			inBatch = false
			pos, err := f.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, false, brokerError(CodeCorruptStorage, "record committed WAL offset", "", err)
			}
			validEnd = pos
		default:
			return nil, false, brokerError(CodeCorruptStorage, "unknown WAL frame kind", fmt.Sprintf("kind=%d offset=%d", frame.kind, startOffset), nil)
		}
	}

	if inBatch {
		if err := trimTornTail(path, validEnd); err != nil {
			return nil, false, err
		}
		return entries, true, nil
	}
	return entries, false, nil
}

var errTornFrame = errors.New("torn WAL frame")

func readFrame(r io.Reader) (int64, walFrame, error) {
	seeker, _ := r.(io.Seeker)
	start, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, walFrame{}, brokerError(CodeCorruptStorage, "read WAL frame offset", "", err)
	}

	header := make([]byte, 13)
	if _, err := io.ReadFull(r, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return start, walFrame{}, errTornFrame
		}
		return start, walFrame{}, brokerError(CodeCorruptStorage, "read WAL frame header", "", err)
	}
	if binary.BigEndian.Uint32(header[:4]) != frameMagic {
		return start, walFrame{}, brokerError(CodeCorruptStorage, "bad WAL magic", "", nil)
	}
	kind := header[4]
	length := int(binary.BigEndian.Uint32(header[5:9]))
	expectedCRC := binary.BigEndian.Uint32(header[9:13])

	payload := make([]byte, length)
	if length > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return start, walFrame{}, errTornFrame
			}
			return start, walFrame{}, brokerError(CodeCorruptStorage, "read WAL frame payload", "", err)
		}
	}
	if crc32.ChecksumIEEE(payload) != expectedCRC {
		return start, walFrame{}, brokerError(CodeCorruptStorage, "WAL frame checksum mismatch", "", nil)
	}
	return start, walFrame{kind: kind, payload: payload}, nil
}

func trimTornTail(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return brokerError(CodeCorruptStorage, "open WAL to trim torn tail", path, err)
	}
	defer f.Close()
	if err := f.Truncate(offset); err != nil {
		return brokerError(CodeCorruptStorage, "trim torn WAL tail", path, err)
	}
	if err := f.Sync(); err != nil {
		return brokerError(CodeCorruptStorage, "sync trimmed WAL", path, err)
	}
	return syncDir(filepath.Dir(path))
}

func removeTemporaryFiles(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return brokerError(CodeCorruptStorage, "read storage directory", dir, err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != tmpSuffix {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		if err := os.Remove(path); err != nil {
			return brokerError(CodeCorruptStorage, "remove temporary file", path, err)
		}
	}
	return syncDir(dir)
}
