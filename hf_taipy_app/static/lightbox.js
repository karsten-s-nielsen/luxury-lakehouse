(function(){
  let _previouslyFocused = null;
  let _overlay = null;
  let _overlayImg = null;
  let _gallery = [];
  let _idx = -1;

  function gatherGallery() {
    return Array.from(document.querySelectorAll('.ll-content-row img'));
  }

  function showAt(index) {
    if (!_overlay || !_overlayImg || _gallery.length === 0) return;
    const n = _gallery.length;
    _idx = ((index % n) + n) % n;
    const target = _gallery[_idx];
    _overlayImg.src = target.src;
    _overlayImg.alt = target.alt || '';
  }

  function goNext() { showAt(_idx + 1); }
  function goPrev() { showAt(_idx - 1); }

  function trapFocus(e) {
    if (!_overlay) return;
    if (e.key === 'Escape') { e.preventDefault(); closeOverlay(); return; }
    if (e.key === 'ArrowRight') { e.preventDefault(); goNext(); return; }
    if (e.key === 'ArrowLeft') { e.preventDefault(); goPrev(); return; }
    if (e.key !== 'Tab') return;
    const focusables = _overlay.querySelectorAll('button, [tabindex="0"]');
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  }

  function closeOverlay() {
    if (!_overlay) return;
    document.removeEventListener('keydown', trapFocus);
    _overlay.remove();
    _overlay = null;
    _overlayImg = null;
    _gallery = [];
    _idx = -1;
    if (_previouslyFocused) { _previouslyFocused.focus(); _previouslyFocused = null; }
  }

  function openOverlay(img) {
    _previouslyFocused = document.activeElement;
    _gallery = gatherGallery();
    _idx = _gallery.indexOf(img);
    if (_idx < 0) _idx = 0;

    const scopeEl = document.querySelector('.ll-page-scope, [data-role="page-scope"]');

    _overlay = document.createElement('div');
    _overlay.className = 'll-lightbox-overlay';
    _overlay.setAttribute('role', 'dialog');
    _overlay.setAttribute('aria-modal', 'true');
    _overlay.setAttribute('aria-label', 'Enlarged chart view');

    const closeBtn = document.createElement('button');
    closeBtn.className = 'll-lightbox-close';
    closeBtn.setAttribute('aria-label', 'Close (Escape)');
    closeBtn.textContent = '\u00d7';
    closeBtn.addEventListener('click', closeOverlay);

    const fig = document.createElement('figure');

    // Scope caption FIRST in DOM so it sits above the image inside the
    // flex column. The CSS `.ll-lightbox-caption` handles the prominent
    // bar styling (left-accent, uppercase labels, bold values).
    if (scopeEl) {
      const caption = document.createElement('figcaption');
      caption.className = 'll-lightbox-caption';
      caption.appendChild(scopeEl.cloneNode(true));
      fig.appendChild(caption);
    }

    _overlayImg = document.createElement('img');
    _overlayImg.src = img.src;
    _overlayImg.alt = img.alt || '';
    fig.appendChild(_overlayImg);

    _overlay.appendChild(closeBtn);
    _overlay.appendChild(fig);

    // On-screen prev/next buttons for gallery navigation. Only render when
    // there is more than one chart to step through.
    if (_gallery.length > 1) {
      const prev = document.createElement('button');
      prev.className = 'll-lightbox-nav ll-lightbox-nav-prev';
      prev.setAttribute('aria-label', 'Previous chart (Left arrow)');
      prev.textContent = '\u2039';
      prev.addEventListener('click', function(e){ e.stopPropagation(); goPrev(); });
      _overlay.appendChild(prev);

      const next = document.createElement('button');
      next.className = 'll-lightbox-nav ll-lightbox-nav-next';
      next.setAttribute('aria-label', 'Next chart (Right arrow)');
      next.textContent = '\u203a';
      next.addEventListener('click', function(e){ e.stopPropagation(); goNext(); });
      _overlay.appendChild(next);
    }

    _overlay.addEventListener('click', function(e) {
      if (e.target === _overlay) closeOverlay();
    });

    document.body.appendChild(_overlay);
    document.addEventListener('keydown', trapFocus);
    closeBtn.focus();
  }

  document.addEventListener('click', function(e) {
    const img = e.target;
    if (img.tagName !== 'IMG') return;
    if (!img.closest('.ll-content-row')) return;
    if (_overlay) return;
    e.stopPropagation();
    openOverlay(img);
  });

  document.addEventListener('keydown', function(e) {
    if (e.key !== 'Enter') return;
    const el = document.activeElement;
    if (el && el.tagName === 'IMG' && el.closest('.ll-content-row')) {
      e.preventDefault();
      openOverlay(el);
    }
  });

  var observer = new MutationObserver(function() {
    document.querySelectorAll('.ll-content-row img:not([tabindex])').forEach(function(img) {
      img.setAttribute('tabindex', '0');
    });
  });
  observer.observe(document.body, { childList: true, subtree: true });
})();
