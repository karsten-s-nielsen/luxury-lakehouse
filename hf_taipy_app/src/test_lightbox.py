"""Manual / MCP Puppeteer scenario for the lightbox rewrite.

This file documents the steps to execute via ``mcp__puppeteer`` when running
the Heat Map UI cycle verification. It is NOT a standalone pytest test because
the Puppeteer MCP tool operates at the Claude-tool layer, not the pytest
layer. It is here as the canonical checklist.

Steps (execute via mcp__puppeteer in order):

1. puppeteer_navigate to http://localhost:7860/Heat-Map
2. Wait for .ll-page-scope to appear — evaluate
   ``document.querySelectorAll('.ll-page-scope').length === 1``
3. Click any .ll-content-row img — expect .ll-lightbox-overlay to appear.
4. Evaluate:
     const overlay = document.querySelector('.ll-lightbox-overlay');
     overlay.getAttribute('role') === 'dialog' &&
     overlay.getAttribute('aria-modal') === 'true' &&
     overlay.querySelector('figure') !== null &&
     overlay.querySelector('figcaption') !== null
   Expect true.
5. Evaluate ``document.querySelector('.ll-lightbox-caption').textContent``
   includes the current competition label.
6. Send Escape key — expect overlay to be removed.
7. Repeat click → verify overlay reappears.
8. Tab key inside overlay — focus must not leave the overlay.
9. Close overlay via close button click — expect focus returns to the
   original image.
10. Change filter (competition), click a different image → figcaption
    reflects new scope.
"""
