// admin.js — the small bits of interactivity htmx doesn't cover, in plain JS
// with no build step: closing the slide-over, switcher-select navigation, and
// add/remove of repeated form items. Everything is event-delegated off document
// so it works on htmx-swapped content (the detail panel) without re-binding.

(function() {
	'use strict';

	const panelRoot = () => document.getElementById('panel-root');

	// Close the slide-over by emptying its mount point.
	function closePanel() {
		const root = panelRoot();
		if (root) root.replaceChildren();
	}

	// Clicks: panel close controls, repeated add, repeated remove.
	document.addEventListener('click', function(e) {
		const closeEl = e.target.closest('[data-panel-close]');
		if (closeEl) {
			e.preventDefault();
			closePanel();
			return;
		}

		const addEl = e.target.closest('[data-repeat-add]');
		if (addEl) {
			e.preventDefault();
			addRepeatItem(addEl.getAttribute('data-repeat-add'), addEl);
			return;
		}

		const removeEl = e.target.closest('[data-repeat-remove]');
		if (removeEl) {
			e.preventDefault();
			// Nearest wins, so a sub-condition's ✕ takes only that sub-row while
			// the group head's ✕ (outside .filter-sub) takes the whole group.
			const item = removeEl.closest('.list-item, .chip, .kv-row, .filter-sub, .filter-cond');
			if (item) item.remove();
			return;
		}

		const refAddEl = e.target.closest('[data-ref-add]');
		if (refAddEl) {
			e.preventDefault();
			const fs = refAddEl.closest('.ref-field');
			const picker = fs && fs.querySelector('[data-ref-picker]');
			if (picker && !picker.hidden) {
				closeRefPicker(fs);
			} else {
				closeAllRefPickers(fs);
				openRefPicker(fs);
			}
			return;
		}

		const resultEl = e.target.closest('[data-ref-result]');
		if (resultEl) {
			e.preventDefault();
			selectRef(resultEl);
			return;
		}

		const themeEl = e.target.closest('[data-theme-toggle]');
		if (themeEl) {
			e.preventDefault();
			applyTheme(effectiveTheme() === 'dark' ? 'light' : 'dark');
			return;
		}

		// A click anywhere else dismisses an open picker (outside-click close).
		const openPicker = document.querySelector('[data-ref-picker]:not([hidden])');
		if (openPicker && !openPicker.contains(e.target)) {
			closeRefPicker(openPicker.closest('.ref-field'));
		}
	});

	// Keyboard for the reference combobox + the slide-over: in the search input
	// the arrow keys move the highlighted option and Enter selects it (never
	// submitting the form); Escape closes an open picker, else the panel.
	document.addEventListener('keydown', function(e) {
		const search = e.target.closest && e.target.closest('.ref-search');
		if (search) {
			const fs = search.closest('.ref-field');
			if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
				e.preventDefault();
				moveActiveResult(fs, e.key === 'ArrowDown' ? 1 : -1);
				return;
			}
			if (e.key === 'Enter') {
				e.preventDefault();
				const active = activeResult(fs) || fs.querySelector('[data-ref-result]');
				if (active) selectRef(active);
				return;
			}
		}
		if (e.key === 'Escape') {
			// An open reference picker closes first, leaving the panel open.
			const picker = document.querySelector('[data-ref-picker]:not([hidden])');
			if (picker) {
				e.preventDefault();
				closeRefPicker(picker.closest('.ref-field'));
				return;
			}
			const root = panelRoot();
			if (root && root.childElementCount > 0) {
				e.preventDefault();
				closePanel();
			}
		}
	});

	// Switcher <select>s carry target URLs as option values; navigate on change.
	document.addEventListener('change', function(e) {
		const sel = e.target.closest('select[data-nav]');
		if (sel && sel.value) location.href = sel.value;
	});

	// Focus the first field when a panel is swapped in; drop already-selected
	// options whenever a reference picker's list (re)loads.
	document.body.addEventListener('htmx:afterSwap', function(e) {
		if (!e.target) return;
		if (e.target.id === 'panel-root') {
			const first = e.target.querySelector(
				'input:not([type=hidden]):not([readonly]), textarea, select',
			);
			if (first) first.focus();
			return;
		}
		if (e.target.matches && e.target.matches('[data-ref-results]')) {
			filterSelectedResults(e.target.closest('.ref-field'));
		}
	});

	// addRepeatItem clones a field's <template data-repeat-tpl> into its items
	// container, substituting a fresh index for the __i__ placeholder in every
	// input name. The index counter only grows, so removals never cause clashes.
	//
	// The template is matched by NAME, not just by attribute: repeat groups
	// nest (a filter condition group sits inside the filter bar's own group),
	// and a plain descendant lookup would find an inner group's template first.
	function addRepeatItem(name, addEl) {
		const fs = addEl.closest('[data-repeat]');
		if (!fs) return;
		const items = fs.querySelector('[data-repeat-items]');
		const tpl = fs.querySelector('template[data-repeat-tpl="' + CSS.escape(name) + '"]');
		if (!items || !tpl) return;

		const next = fs.dataset.nextIndex !== undefined
			? parseInt(fs.dataset.nextIndex, 10)
			: items.children.length;
		fs.dataset.nextIndex = String(next + 1);

		const frag = tpl.content.cloneNode(true);
		frag.querySelectorAll('[name]').forEach(function(el) {
			el.setAttribute('name', el.getAttribute('name').replaceAll('__i__', String(next)));
		});
		frag.querySelectorAll('[id], [list], [for]').forEach(function(el) {
			[ 'id', 'list', 'for' ].forEach(function(attr) {
				const v = el.getAttribute(attr);
				if (v) el.setAttribute(attr, v.replaceAll('__i__', String(next)));
			});
		});
		items.appendChild(frag);
		if (window.htmx) window.htmx.process(items.lastElementChild);
	}

	// openRefPicker reveals a ref field's combobox popover and focuses its search
	// input, which fires the htmx focus-trigger that loads the initial options.
	function openRefPicker(fs) {
		if (!fs) return;
		const picker = fs.querySelector('[data-ref-picker]');
		if (!picker) return;
		picker.hidden = false;
		setRefAddExpanded(fs, true);
		const search = picker.querySelector('.ref-search');
		if (search) search.focus();
		positionRefPicker(fs);
	}

	// positionRefPicker flips the popover above its row when the options would
	// otherwise spill below the visible panel, and caps the option list so the
	// whole popover fits the side it lands on.
	function positionRefPicker(fs) {
		const picker = fs && fs.querySelector('[data-ref-picker]');
		if (!picker || picker.hidden) return;
		const row = fs.querySelector('.ref-row');
		const results = picker.querySelector('[data-ref-results]');
		if (results) results.style.maxHeight = '';
		picker.classList.remove('ref-pop--up');

		const container = fs.closest('.panel-body');
		const cRect = container
			? container.getBoundingClientRect()
			: { top: 0, bottom: window.innerHeight };
		const rowRect = row.getBoundingClientRect();
		const margin = 10, gap = 6;
		const spaceBelow = cRect.bottom - rowRect.bottom - margin;
		const spaceAbove = rowRect.top - cRect.top - margin;

		const up = (picker.offsetHeight + gap) > spaceBelow && spaceAbove > spaceBelow;
		if (up) picker.classList.add('ref-pop--up');

		if (results) {
			const inputH = (picker.querySelector('.ref-search') || {}).offsetHeight || 40;
			const space = (up ? spaceAbove : spaceBelow) - inputH - gap;
			if (results.scrollHeight > space) results.style.maxHeight = Math.max(96, space) + 'px';
		}
	}

	// closeRefPicker hides the popover again and clears its query + options, so it
	// collapses back to the "+ Add" control.
	function closeRefPicker(fs) {
		if (!fs) return;
		const picker = fs.querySelector('[data-ref-picker]');
		if (!picker) return;
		picker.hidden = true;
		setRefAddExpanded(fs, false);
		const search = picker.querySelector('.ref-search');
		if (search) search.value = '';
		const results = picker.querySelector('[data-ref-results]');
		if (results) results.replaceChildren();
	}

	// closeAllRefPickers collapses every open picker except the one in `keep`, so
	// at most one combobox is open at a time.
	function closeAllRefPickers(keep) {
		document.querySelectorAll('[data-ref-picker]:not([hidden])').forEach(function(p) {
			const fs = p.closest('.ref-field');
			if (fs !== keep) closeRefPicker(fs);
		});
	}

	function setRefAddExpanded(fs, on) {
		const btn = fs.querySelector('[data-ref-add]');
		if (btn) btn.setAttribute('aria-expanded', on ? 'true' : 'false');
	}

	// selectRef turns a clicked picker result into a selected reference chip: it
	// clones the field's chip template, fills the label and the hidden key inputs
	// from the result's data-keys, and either replaces (singular) or appends
	// (repeated) it. Input names follow the bind contract — Name.<key> for a
	// singular ref, Name[i].<key> for a repeated one.
	function selectRef(resultEl) {
		const fs = resultEl.closest('.ref-field');
		if (!fs) return;
		const items = fs.querySelector('[data-ref-items]');
		const tpl = fs.querySelector('template[data-ref-tpl]');
		if (!items || !tpl) return;

		let keys = {};
		try {
			keys = JSON.parse(resultEl.getAttribute('data-keys') || '{}');
		} catch (_) { /* leave keys empty */ }
		const label = resultEl.getAttribute('data-label') || '';
		const repeated = fs.hasAttribute('data-ref-repeated');

		// The chip template names inputs Name[__i__].<key>; substitute a fresh
		// index for repeated refs, or drop the bracket entirely for singular ones.
		let idxRepl = '';
		if (repeated) {
			if (hasSameRef(items, keys)) return; // already selected
			const next = fs.dataset.nextIndex !== undefined
				? parseInt(fs.dataset.nextIndex, 10)
				: items.children.length;
			fs.dataset.nextIndex = String(next + 1);
			idxRepl = '[' + next + ']';
		}

		const frag = tpl.content.cloneNode(true);
		const chip = frag.querySelector('[data-ref-chip]');
		if (!chip) return;
		chip.querySelectorAll('input[data-ref-key]').forEach(function(el) {
			el.setAttribute('name', el.getAttribute('name').replace('[__i__]', idxRepl));
			const k = el.getAttribute('data-ref-key');
			if (Object.prototype.hasOwnProperty.call(keys, k)) el.value = keys[k];
		});
		const labelEl = chip.querySelector('[data-ref-label]');
		if (labelEl) labelEl.textContent = label;

		// Render the new chip exactly like a server-rendered one: the blank
		// template carries only a neutral dot, so adopt the result's color slot
		// and its avatar initials (when it has them).
		const slot = (resultEl.className.match(/\b(?:c\d+|neutral)\b/) || [])[0];
		if (slot) {
			chip.className = (chip.className.replace(/\b(?:c\d+|neutral)\b/g, '').trim() + ' ' + slot)
				.trim();
		}
		const resAvatar = resultEl.querySelector('.avatar');
		const badge = chip.querySelector('.avatar, .dot');
		if (resAvatar && badge) {
			const av = document.createElement('span');
			av.className = 'avatar sm';
			av.textContent = resAvatar.textContent;
			badge.replaceWith(av);
		}

		if (repeated) {
			// Keep the popover open to add several in a row: append the chip, drop
			// the chosen option from the list, and refocus the filter input.
			items.appendChild(chip);
			resultEl.remove();
			markExhaustedIfEmpty(fs);
			updateActiveResult(fs, null);
			positionRefPicker(fs);
			const search = fs.querySelector('.ref-search');
			if (search) search.focus();
		} else {
			// A singular ref holds one value, so collapse the popover once chosen.
			items.replaceChildren(chip);
			closeRefPicker(fs);
		}
	}

	// filterSelectedResults removes options already chosen in this field, so the
	// list never shows a dead (no-op) row; it runs after every options load.
	function filterSelectedResults(fs) {
		const chosen = selectedKeySets(fs);
		fs.querySelectorAll('[data-ref-result]').forEach(function(r) {
			let keys = {};
			try {
				keys = JSON.parse(r.getAttribute('data-keys') || '{}');
			} catch (_) { /* skip */ }
			if (
				chosen.some(function(s) {
					return keysEqual(s, keys);
				})
			) r.remove();
		});
		markExhaustedIfEmpty(fs);
		updateActiveResult(fs, fs.querySelector('[data-ref-result]'));
		positionRefPicker(fs);
	}

	// markExhaustedIfEmpty shows a quiet note when no addable option remains, so
	// an empty list still reads as a state rather than a glitch.
	function markExhaustedIfEmpty(fs) {
		const results = fs.querySelector('[data-ref-results]');
		if (
			!results || results.querySelector('[data-ref-result]') || results.querySelector('.ref-empty')
		) return;
		const note = document.createElement('div');
		note.className = 'ref-empty';
		note.textContent = 'Nothing left to add';
		results.appendChild(note);
	}

	// pointsElsewhere reports whether a chip names another workspace. Picker
	// rows always come from this workspace, so such a chip matches none of them
	// whatever its key says.
	function pointsElsewhere(chip) {
		const ws = chip.querySelector('input[data-ref-workspace]');
		return ws !== null && ws.value !== '';
	}

	// selectedKeySets returns the key-field maps of every chip already in the
	// field that a picker option could match.
	function selectedKeySets(fs) {
		const items = fs.querySelector('[data-ref-items]');
		if (!items) return [];
		const chips = Array.prototype.filter.call(items.querySelectorAll('[data-ref-chip]'), function(chip) {
			return !pointsElsewhere(chip);
		});
		return chips.map(function(chip) {
			const obj = {};
			chip.querySelectorAll('input[data-ref-key]').forEach(function(el) {
				obj[el.getAttribute('data-ref-key')] = el.value;
			});
			return obj;
		});
	}

	function keysEqual(a, b) {
		const ak = Object.keys(a);
		if (ak.length === 0 || ak.length !== Object.keys(b).length) return false;
		return ak.every(function(k) {
			return a[k] === b[k];
		});
	}

	// activeResult / moveActiveResult / updateActiveResult drive the keyboard
	// highlight over the option list (arrow keys + Enter).
	function activeResult(fs) {
		return fs ? fs.querySelector('[data-ref-result].active') : null;
	}

	function moveActiveResult(fs, delta) {
		const all = Array.prototype.slice.call(fs.querySelectorAll('[data-ref-result]'));
		if (all.length === 0) return;
		const cur = activeResult(fs);
		let i = cur ? all.indexOf(cur) : -1;
		i = (i + delta + all.length) % all.length;
		updateActiveResult(fs, all[i]);
		all[i].scrollIntoView({ block: 'nearest' });
	}

	function updateActiveResult(fs, next) {
		fs.querySelectorAll('[data-ref-result].active').forEach(function(el) {
			el.classList.remove('active');
		});
		if (next) next.classList.add('active');
	}

	// hasSameRef reports whether items already holds a chip with the given key
	// values, so a repeated reference is not selected twice.
	function hasSameRef(items, keys) {
		return Array.prototype.some.call(items.querySelectorAll('[data-ref-chip]'), function(chip) {
			if (pointsElsewhere(chip)) return false;
			const inputs = chip.querySelectorAll('input[data-ref-key]');
			if (inputs.length === 0) return false;
			return Array.prototype.every.call(inputs, function(el) {
				return keys[el.getAttribute('data-ref-key')] === el.value;
			});
		});
	}

	// ---- Theme ---------------------------------------------------------------
	// An explicit choice lives in [data-theme] on <html> (the <head> applies it
	// before paint) and in localStorage; its absence means follow the OS. We only
	// keep the toggle glyph in sync and flip the stored choice on click — the CSS
	// light-dark() pairs do the actual recoloring off the resolved color-scheme.

	const darkQuery = window.matchMedia('(prefers-color-scheme: dark)');

	function effectiveTheme() {
		const forced = document.documentElement.getAttribute('data-theme');
		if (forced === 'dark' || forced === 'light') return forced;
		return darkQuery.matches ? 'dark' : 'light';
	}

	// Glyph shows the destination of a click: a sun in dark mode (→ light), a
	// moon in light mode (→ dark).
	function syncThemeIcon() {
		const glyph = effectiveTheme() === 'dark' ? '☀' : '☾';
		document.querySelectorAll('[data-theme-icon]').forEach(function(el) {
			el.textContent = glyph;
		});
	}

	function applyTheme(theme) {
		if (theme !== 'dark' && theme !== 'light') return;
		document.documentElement.setAttribute('data-theme', theme);
		try {
			localStorage.setItem('admin-theme', theme);
		} catch (_) { /* ignore */ }
		syncThemeIcon();
	}

	syncThemeIcon();
	// While following the OS (no explicit choice), track its changes live.
	darkQuery.addEventListener('change', function() {
		if (!document.documentElement.getAttribute('data-theme')) syncThemeIcon();
	});
})();
