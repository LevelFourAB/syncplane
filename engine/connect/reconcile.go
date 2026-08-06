package connect

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/levelfourab/syncplane/engine/internal/lww"
	"github.com/levelfourab/syncplane/engine/server"
	"github.com/levelfourab/syncplane/engine/text"
)

// Reconcile runs one full convergence pass for the workspace: it scans the
// source, ingresses every change made there since the last sync (field-
// precisely, so concurrent client edits to other fields survive), tombstones
// rows deleted at the source, and repairs any bookkeeping a crash left
// stale. Unchanged items cost one fingerprint compare.
//
// The first Reconcile over an empty shadow set is the initial import: every
// source item flows into the engine, and the engine's pre-existing rows flow
// out on the next Drain.
//
// An Incremental source normally reaches this through Ingress, which runs it
// only when there is no live cursor or the safety-net interval is due.
// Calling it directly forces the enumeration but takes no checkpoint, so a
// change racing the scan is caught by the next incremental pass rather than
// stepped over.
func (c *Connector) Reconcile(ctx context.Context, workspaceID string) error {
	return c.converge(ctx, workspaceID, func(p *pass) error {
		return c.reconcile(ctx, workspaceID, p)
	})
}

func (c *Connector) reconcile(ctx context.Context, workspaceID string, p *pass) error {
	seen := make(map[string]map[string]bool, len(c.bound))
	for entity := range c.bound {
		seen[entity] = make(map[string]bool)
	}
	if err := p.src.List(ctx, workspaceID, func(item Item) error {
		entity, key, err := c.ingestItem(ctx, workspaceID, p, item)
		if err != nil {
			return err
		}
		// Marked even when ingest skipped the item (undecodable text): the
		// row is still there, and the sweep below must not tombstone it.
		seen[entity][key] = true
		return nil
	}); err != nil {
		return err
	}

	// Shadows for rows the source no longer lists mark deletions at the
	// source. The sweep belongs to the full pass alone: an incremental pass
	// sees only what Changes reported, where a row's absence from a delta
	// means "unchanged", not "deleted". Collect first — State implementations
	// need not support reentrant calls under an open scan.
	type rowID struct{ entity, key string }
	var gone []rowID
	if err := c.state.ScanShadows(ctx, workspaceID, func(entity, key string, _ Shadow) error {
		if set, ok := seen[entity]; ok && !set[key] {
			gone = append(gone, rowID{entity, key})
		}
		return nil
	}); err != nil {
		return err
	}
	for _, id := range gone {
		// Arrival-order LWW, like every write: the delete reaches the
		// linearization point now; a concurrent client edit that lands later
		// recreates the row.
		if err := c.srv.Mutate(server.TrustedAs(ctx, c.label), workspaceID, func(tx *server.EntityTx) error {
			return tx.Delete(c.bound[id.entity].prototype, id.key)
		}); err != nil {
			return err
		}
		// The tombstone redelivers through the next Drain and egresses as an
		// idempotent no-op delete.
		if err := c.state.DeleteShadow(ctx, workspaceID, id.entity, id.key); err != nil {
			return err
		}
	}
	if len(gone) > 0 {
		c.logger.LogAttrs(ctx, slog.LevelDebug, "reconcile tombstoned source deletions",
			slog.String("workspace", workspaceID), slog.Int("deletions", len(gone)))
	}
	return nil
}

// ingest converges one source item into the engine. The shadow's base is the
// three-way ancestor: only fields the source actually changed are applied,
// so the engine's per-field LWW merges a concurrent client edit instead of
// losing it.
//
// What that base MEANS depends on the binding's direction. Bidirectional
// ingest writes the merge back, so "the last agreed row" and "what the source
// holds" are the same bytes. IngressOnly never writes back, so the two
// diverge the moment a client edits the row — and the base is therefore
// stored as the SOURCE's view (the item), because a base carrying client-only
// values would read as a source edit on the next pass and clobber them.
func (c *Connector) ingest(ctx context.Context, workspaceID string, p *pass, b binding, entity, key string, item Item) error {
	sh, err := c.state.Shadow(ctx, workspaceID, entity, key)
	if err != nil {
		return err
	}
	// Unchanged source content — or the echo of the connector's own egress
	// write coming back through List.
	if sh != nil && sh.SourceFP == item.Fingerprint {
		return nil
	}
	// baseMsg == nil means "no usable ancestor": apply the item whole.
	var (
		baseMsg      proto.Message
		paths        []string
		changedTexts map[string]string
	)
	if sh != nil && len(sh.Base) > 0 {
		base, err := c.decodeBase(b.prototype, sh.Base)
		if err != nil {
			return err
		}
		baseMsg = base
		paths = fieldDiff(base, item.Msg)
		if changedTexts, err = changedTextFields(base, sh.BaseText, item.TextFields); err != nil {
			return err
		}
		if len(paths) == 0 && len(changedTexts) == 0 {
			// Content identical, fingerprint moved (an mtime touch, a crash
			// between Source.Write and PutShadow): repair the fingerprint.
			return c.state.PutShadow(ctx, workspaceID, entity, key, Shadow{
				SourceFP:      item.Fingerprint,
				EngineVersion: sh.EngineVersion,
				Base:          sh.Base,
				BaseText:      sh.BaseText,
			})
		}
	} else {
		// No ancestor: everything the source reports is new to us.
		changedTexts = item.TextFields
	}

	var (
		seq    uint64
		merged proto.Message
	)
	if err := c.srv.Mutate(server.TrustedAs(ctx, c.label), workspaceID, func(tx *server.EntityTx) error {
		// Reset on entry: the storage adapter may retry this closure.
		seq, merged = 0, nil
		cur := b.prototype.ProtoReflect().New().Interface()
		found, err := tx.Get(cur, key)
		if err != nil {
			return err
		}
		if !found || baseMsg == nil {
			merged = proto.Clone(item.Msg)
		} else {
			merged = proto.Clone(cur)
			applyFields(merged, item.Msg, paths)
		}
		var curForText proto.Message
		if found {
			curForText = cur
		}
		if err := c.applyTextFields(merged, baseMsg, curForText, b.direction, item.TextFields, changedTexts); err != nil {
			return err
		}
		if found && proto.Equal(merged, cur) {
			// The engine already holds this state (e.g. a crash after
			// commit but before the shadow update): no write, no seq.
			return nil
		}
		if err := tx.Put(merged); err != nil {
			return err
		}
		seq = tx.Seq()
		return nil
	}); err != nil {
		return err
	}
	if seq != 0 {
		if err := c.pace(ctx, p); err != nil {
			return err
		}
	}

	fp := item.Fingerprint
	baseMsgOut := merged
	baseText := item.TextFields
	if b.direction == Bidirectional {
		// When the merge kept concurrent client edits, the engine's merged
		// row differs from what the source holds — push it back out now so
		// the two sides genuinely agree before the shadow says they do.
		// Without this, the shadow's EngineVersion would suppress the very
		// change that carries the client's fields to the source. Text fields
		// compare by their visible string (the only representation the source
		// has).
		same, err := sourceViewEqual(merged, item)
		if err != nil {
			return err
		}
		if !same {
			w, err := p.writer()
			if err != nil {
				return err
			}
			if fp, err = w.Write(ctx, workspaceID, merged); err != nil {
				return fmt.Errorf("connect: ingress write-back %s/%s: %w", entity, key, err)
			}
		}
		if baseText, err = plainTextFields(merged); err != nil {
			return err
		}
	} else {
		// Nothing went back to the source, so it still holds exactly what it
		// reported: record that as the base, not the merge.
		baseMsgOut = item.Msg
	}
	base, err := proto.Marshal(baseMsgOut)
	if err != nil {
		return err
	}
	engineVersion := seq
	if seq == 0 && sh != nil {
		// No engine write happened; keep the version we knew.
		engineVersion = sh.EngineVersion
	}
	return c.state.PutShadow(ctx, workspaceID, entity, key, Shadow{
		SourceFP:      fp,
		EngineVersion: engineVersion,
		Base:          base,
		BaseText:      cloneTexts(baseText),
	})
}

// applyTextFields rewrites the named text fields of merged from the plain
// strings the source reported, delegating to server.ApplyTextStrings. The two
// directions need different ancestors:
//
//   - Bidirectional diffs every reported string against the shadow BASE
//     document — the last state the source observed, the true three-way
//     ancestor — and unions the result with the engine's current document, so
//     an external edit and a concurrent client edit both survive. Writing the
//     merge back is what keeps that base honest.
//   - IngressOnly has no shared ancestor once write-back is gone: the base is
//     the source's view and never carries the client's edits. So a field the
//     source did not change is left alone entirely, and one it did change is
//     applied as a minimal diff onto the engine's CURRENT document (ancestor
//     = cur), letting the source win exactly the characters it touched. A
//     client edit to an ingress-only row has nowhere to go by definition.
//
// The writer identity is the connector's, fixed for its lifetime (two
// connectors that can run at the same time must never share one, or two
// processes would allocate colliding item ids). Reusing it across calls — and
// across restarts, when Config.WriterID pins it — is only sound because the
// ancestor document OBSERVES cur's version before it is edited: without that,
// an edit diffed against a stale ancestor would re-spend counters this same
// peer already used in cur. Calls are serialized per row by the engine's
// write exclusion, and cur is read inside that write transaction, so cur
// always carries this peer's earlier writes — including on a retry of the
// transaction closure.
func (c *Connector) applyTextFields(merged, base, cur proto.Message, dir Direction, all, changed map[string]string) error {
	if dir == Bidirectional {
		return server.ApplyTextStrings(merged, base, cur, all, c.peer)
	}
	return server.ApplyTextStrings(merged, nil, cur, changed, c.peer)
}

// invalidTextField names the first text field whose source string the text
// package would refuse, with the offset of the offending byte, or "" when they
// all pass. Fields are checked in name order so a source with more than one
// broken field reports the same one every pass.
func invalidTextField(texts map[string]string) (string, int) {
	names := make([]string, 0, len(texts))
	for name := range texts {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if off := text.InvalidUTF8Offset(texts[name]); off >= 0 {
			return name, off
		}
	}
	return "", 0
}

// changedTextFields returns the subset of the source's reported strings that
// differ from what the source last showed — the text half of the fieldDiff
// decision, since text fields carry no comparable bytes on Item.Msg.
//
// Shadow.BaseText is that record and compares directly, with no decode.
// Shadows written before it existed have none, and are bidirectional by
// construction (there was no other direction), so their base document renders
// to what the source last held: decode it.
func changedTextFields(base proto.Message, baseText, texts map[string]string) (map[string]string, error) {
	var changed map[string]string
	for name, s := range texts {
		fd := base.ProtoReflect().Descriptor().Fields().ByName(protoreflect.Name(name))
		if fd == nil || !lww.IsText(fd) {
			return nil, fmt.Errorf("connect: %s has no text field %q",
				base.ProtoReflect().Descriptor().FullName(), name)
		}
		was := baseText[name]
		if baseText == nil {
			var err error
			if was, err = text.PlainText(base.ProtoReflect().Get(fd).Bytes()); err != nil {
				return nil, err
			}
		}
		if was != s {
			if changed == nil {
				changed = make(map[string]string, len(texts))
			}
			changed[name] = s
		}
	}
	return changed, nil
}

// plainTextFields renders every collaborative-text field of msg to its
// visible string — what a source the row was just written to now holds.
// Returns nil when the entity has no text fields, so entities without text
// store no BaseText at all.
func plainTextFields(msg proto.Message) (map[string]string, error) {
	var out map[string]string
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !lww.IsText(fd) {
			continue
		}
		s, err := text.PlainText(msg.ProtoReflect().Get(fd).Bytes())
		if err != nil {
			return nil, err
		}
		if out == nil {
			out = make(map[string]string, 1)
		}
		out[string(fd.Name())] = s
	}
	return out, nil
}

// cloneTexts copies a text map so a shadow never aliases a source's.
func cloneTexts(texts map[string]string) map[string]string {
	if len(texts) == 0 {
		return nil
	}
	return maps.Clone(texts)
}

// sourceViewEqual reports whether the engine's merged row and the source's
// item agree from the SOURCE's point of view: text fields compare by their
// visible string against Item.TextFields, everything else by proto equality
// with the text fields cleared (the item never carries document bytes).
func sourceViewEqual(merged proto.Message, item Item) (bool, error) {
	a := proto.Clone(merged)
	b := proto.Clone(item.Msg)
	fields := a.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if !lww.IsText(fd) {
			continue
		}
		s, err := text.PlainText(a.ProtoReflect().Get(fd).Bytes())
		if err != nil {
			return false, err
		}
		if s != item.TextFields[string(fd.Name())] {
			return false, nil
		}
		a.ProtoReflect().Clear(fd)
		b.ProtoReflect().Clear(fd)
	}
	return proto.Equal(a, b), nil
}
