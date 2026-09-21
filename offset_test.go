package broker

import "testing"

func TestReplayRejectsOffsetRollbackAndGap(t *testing.T) {
	t.Run("rollback", func(t *testing.T) {
		events := []journalEvent{
			{Type: eventSubscribe, At: timeNowForTest(), Topic: "t", Group: "g"},
			{Type: eventPublish, At: timeNowForTest(), Topic: "t", Key: "k", MessageID: "m1", Offset: 1, Groups: []string{"g"}, Seq: 1},
			{Type: eventTerminal, At: timeNowForTest(), Topic: "t", Group: "g", Key: "k", MessageID: "m1", Offset: 1, Seq: 1, Decision: DecisionAck},
			{Type: eventPublish, At: timeNowForTest(), Topic: "t", Key: "k", MessageID: "m2", Offset: 2, Groups: []string{"g"}, Seq: 2},
			{Type: eventTerminal, At: timeNowForTest(), Topic: "t", Group: "g", Key: "k", MessageID: "m2", Offset: 2, Seq: 1, Decision: DecisionAck},
		}
		_, err := replay(events)
		if !IsCode(err, CodeOffsetRollback) {
			t.Fatalf("rollback replay err=%v, want %s", err, CodeOffsetRollback)
		}
		t.Logf("test input={committed:2 requested:1} decision=reject code=%s reason=offset_rollback", CodeOffsetRollback)
	})

	t.Run("gap", func(t *testing.T) {
		state := newEngineState()
		events := []journalEvent{
			{Type: eventSubscribe, At: timeNowForTest(), Topic: "t", Group: "g"},
			{Type: eventPublish, At: timeNowForTest(), Topic: "t", Key: "k", MessageID: "m1", Offset: 1, Groups: []string{"g"}, Seq: 1},
			{Type: eventTerminal, At: timeNowForTest(), Topic: "t", Group: "g", Key: "k", MessageID: "m1", Offset: 1, Seq: 1, Decision: DecisionAck},
			{Type: eventPublish, At: timeNowForTest(), Topic: "t", Key: "k", MessageID: "m2", Offset: 2, Groups: []string{"g"}, Seq: 2},
			{Type: eventTerminal, At: timeNowForTest(), Topic: "t", Group: "g", Key: "k", MessageID: "m2", Offset: 2, Seq: 3, Decision: DecisionAck},
		}
		if err := state.apply(events[0]); err != nil {
			t.Fatal(err)
		}
		if err := state.apply(events[1]); err != nil {
			t.Fatal(err)
		}
		if err := state.apply(events[2]); err != nil {
			t.Fatal(err)
		}
		if err := state.apply(events[3]); err != nil {
			t.Fatal(err)
		}
		err := state.apply(events[4])
		if !IsCode(err, CodeOffsetRollback) {
			t.Fatalf("gap replay err=%v, want %s", err, CodeOffsetRollback)
		}
		t.Logf("test input={committed:1 requested:3} decision=reject code=%s reason=offset_skip", CodeOffsetRollback)

		ts := state.topics["t"]
		gs := ts.groups["g"]
		ks := gs.keys["k"]
		if ks.committed != 1 || len(ks.queue) != 1 || ks.queue[0] != "m2" {
			t.Fatalf("state polluted by rejected gap: committed=%d queue=%v", ks.committed, ks.queue)
		}
	})
}
