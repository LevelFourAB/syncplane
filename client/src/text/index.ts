export {
	decodeAnchor,
	decodeTextDoc,
	encodeAnchor,
	encodePlainString,
	encodeTextDoc,
	isPlainString,
	plainString,
	plainTextOf,
} from './codec.js';
export {
	compactDoc,
	type CompactResult,
	decodeVV,
	encodeVV,
	purgedByRemote,
} from './compact.js';
export {
	type Anchor,
	AnchorAfter,
	AnchorBefore,
	AnchorDocEnd,
	AnchorDocStart,
	type AnchorKind,
	codePointToUtf16,
	compareIds,
	type Expand,
	ExpandAfter,
	ExpandBefore,
	ExpandBoth,
	ExpandNone,
	type ItemID,
	type MarkValue,
	type PendingMark,
	type Point,
	type SegmentMark,
	type Span,
	TextDoc,
	utf16ToCodePoint,
} from './doc.js';
export {
	applyFragment,
	applyFragmentBytes,
	decodeFragment,
	encodeFragment,
	encodeFragmentFrom,
	isFragment,
	mergeFragments,
	TextFragment,
} from './fragment.js';
export { type DeltaOp, diffText, mergeInto, mergeText, type TextDelta } from './merge.js';
export { textMarksOf, textOf } from './project.js';
