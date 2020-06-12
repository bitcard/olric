package olric

import (
	"context"
	"github.com/buraksezer/olric/internal/protocol"
	"io"
	"math/rand"
	"sync"
)

type streams struct {
	mu sync.RWMutex

	m map[uint64]*stream
}

type stream struct {
	read   chan *protocol.Message
	write  chan *protocol.Message
	ctx    context.Context
	cancel context.CancelFunc
}

func (s *stream ) close() {
	s.cancel()
}

func (db *Olric) readFromStream(conn io.Reader, bufCh chan<- *protocol.Message, errCh chan<- error) {
	defer db.wg.Done()

	for {
		var msg protocol.Message
		err := msg.Read(conn)
		if err != nil {
			errCh <- err
			return
		}
		bufCh <- &msg
	}
}

func (db *Olric) createStreamOperation(req *protocol.Message) *protocol.Message {
	conn, err := req.GetConn()
	if err != nil {
		return req.Error(protocol.StatusInternalServerError, err)
	}

	streamID := rand.Uint64()
	ctx, cancel := context.WithCancel(context.Background())
	db.streams.mu.Lock()
	// TODO: Direction of channels
	s := &stream{
		read:   make(chan *protocol.Message, 1),
		write:  make(chan *protocol.Message, 1),
		ctx:    ctx,
		cancel: cancel,
	}
	db.streams.m[streamID] = s
	db.streams.mu.Unlock()

	errCh := make(chan error, 1)
	bufCh := make(chan *protocol.Message, 1)
	db.wg.Add(1)
	go db.readFromStream(conn, bufCh, errCh)

	defer func() {
		db.streams.mu.Lock()
		delete(db.streams.m, streamID)
		db.streams.mu.Unlock()
	}()

	rq := protocol.NewRequest(protocol.OpStreamCreated)
	rq.Extra = protocol.StreamCreatedExtra{
		StreamID: streamID,
	}
	s.write <- rq
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-db.ctx.Done():
			break loop
		case msg := <-s.write:
			err = msg.Write(conn)
			if err != nil {
				return req.Error(protocol.StatusInternalServerError, err)
			}
		case buf := <-bufCh:
			s.read <- buf
		}
	}

	return req.Success()
}
